# ygz_massive_stock_dab_dbt

Stock market data pipeline built with **Databricks Asset Bundles (DAB)** and **dbt Core**.

## Architecture

massive.com REST API → Landing (raw JSON) → Bronze (structured Delta) → Silver/Gold (dbt Core).

| Layer       | Storage                          | Owner              | What happens here                                              |
| ----------- | --------------------------------- | ------------------ | ---------------------------------------------------------------- |
| **Landing** | Raw JSON in a UC Volume, append-only | Python (`src/ingestion/landing`) | Raw massive.com responses, partitioned by date, with request metadata |
| **Bronze**  | Delta table, fixed schema, unclean | Python/PySpark (`src/ingestion/bronze`, DAB job) | JSON parsed into columns + `_ingested_at`/`_source_file` audit fields |
| **Silver**  | Delta table, cleaned               | dbt Core            | UTC normalization, dedup, naming conventions, dbt tests          |
| **Gold**    | Delta table, analysis-ready        | dbt Core            | Daily returns, moving averages/trend, volume anomalies, ticker dimension |

Watchlist is a fixed 10 tickers (see `plan/requirements/requirement_breakdown.md`, local-only) - all 5
massive.com endpoints (daily bars, ticker overview, splits, dividends, news) are landed and
Bronze-loaded for each, but not all on the same schedule: `land_daily_data` (daily_bars + news, the
two sources that actually change day to day) runs daily, and `land_reference_data`
(ticker_overview/splits/dividends, which the vendor returns as full history on every call regardless
of how often you ask) runs weekly - splitting them cut a single run from 50 massive.com calls down to
20 for the job that actually needs to run daily (see
`plan/records/06_job_serverless_process.md`). Bronze is what actually guarantees no duplicates
either way: each `load_*_to_bronze` anti-joins against each table's natural key before appending, so
re-running a load is safe regardless of whether Landing handed it genuinely new data or the same
history again. See `ingestion/bronze/loader.py`'s module docstring and
`plan/records/05_bronze_landing_incremental_process.md` for the full story, including a first design
(partition-based filtering) that looked right but wasn't. Silver and Gold models are both aligned
with the real Bronze schema; `dim_ticker` is a proper SCD2 dimension via a dbt snapshot.

`dev` is the only target that calls massive.com for real - `ingestion_daily_job`/
`ingestion_reference_job` are only defined under `targets.dev` in `databricks.yml`, not the shared
`resources/jobs.yml`, so they structurally don't exist under `-t test`/`-t prod` at all (DAB has no
"exclude this resource from a target" mechanism - defining a resource only inside one target's own
block is the supported way to scope it). `test` and `prod` both get real data via
`promote_from_dev_job` instead: it copies dev's Landing Volume into that target's, then runs
`load_bronze` against that target's catalog - same promotion logic for both, on demand, not on a
schedule. See `plan/records/09_bronze_promotion_process.md`.

## Repo layout

`src/ingestion/` splits into one subpackage per medallion layer. Each layer has pure
functions in its own module, and - only where a real DAB job needs to call into it - a thin
`job.py` with just the entry point (imports the real logic, no logic of its own). This is
the only place `main()`-shaped code lives; everything else is importable functions.

```
databricks.yml              # DAB bundle root config (targets: dev/test/prod, same workspace).
                             #   targets.dev.resources.jobs: ingestion_daily_job,
                             #   ingestion_reference_job (Landing+Bronze, dev-only - see Architecture).
                             #   targets.test/targets.prod.resources.jobs: promote_from_dev_job
resources/
  jobs.yml                  # dbt_job (Silver+Gold) - the only job shared across all 3 targets,
                             #   serverless
src/ingestion/
  settings.py                 # env-var driven runtime config, shared across layers
  landing/
    client.py                   # MassiveClient: auth, retry/backoff, pagination, inter-request delay
    writer.py                    # land_*() + write_landing_records*() - pure functions
    job.py                         # land_daily_data()/land_reference_data() - DAB job entry points
  bronze/
    loader.py                    # load_*_to_bronze() - pure functions
    job.py                         # load_bronze() - the load_bronze DAB job entry point
  promotion/
    copy_landing.py               # copy_landing_volume() - pure function, file-by-file dev -> target
    job.py                          # copy_landing_from_dev() - the promote_from_dev_job DAB job entry point
tests/
  test_massive_client.py          # MassiveClient logic against a fake session
  test_bronze_loader.py            # _exclude_existing_keys bootstrap case (no real Spark locally)
  test_promotion.py                 # copy_landing_volume() against a fake dbutils
  test_settings.py                   # Settings catalog override / from_job_argv precedence
scripts/
  landing/                      # one-time/rerunnable backfill scripts, one per source
  bronze/
    reload_bronze.py               # drop + reload all 6 Bronze tables from current Landing data
    verify_incremental.py            # rerun load_bronze() twice, assert row counts don't move
  dbc_connection_check.py / massive_api_check.py / setup_env*.ps1   # not layer-specific
dbt/
  dbt_project.yml
  profiles.yml                # local-dev only; DAB dbt_task auto-generates its own
  macros/
    generate_schema_name.sql     # custom +schema maps directly to silver/gold, no doubling
  models/silver/               # stg_daily_bars, stg_ticker_overview, stg_splits, stg_dividends,
                                #   stg_news_articles, stg_news_sentiment - aligned with Bronze
  models/gold/                  # dim_ticker, dim_date, dim_publisher, dim_industry,
                                #   fct_daily_bars, fct_ticker_daily_metrics, fct_splits,
                                #   fct_dividends, fct_news_sentiment, fct_daily_returns,
                                #   fct_moving_averages, fct_volume_anomalies - aligned with Bronze
  snapshots/
    dim_ticker_snapshot.sql      # SCD2 source for dim_ticker (strategy=check)
  tests/
    assert_dim_ticker_single_current_version.sql   # singular test: SCD2 invariant
.github/workflows/
  ci-dev.yml                   # push (any branch but main): validate dev only; pull_request: deploy dev
  cd-test-prod.yml               # push to main: deploy test; workflow_dispatch: deploy prod (manual)
```

## Local setup

Two separate virtual environments, on two different Python versions — `databricks-connect`
and `pyspark` can't coexist in the same environment, and Databricks Connect needs a newer
Python than the rest of the project:

| Venv         | Python | Purpose                                                        | Rebuild with                   |
| ------------ | ------ | ---------------------------------------------------------------- | ------------------------------- |
| `.venv`      | 3.11   | Ingestion unit tests, dbt Core, local pyspark logic (`pip install -e ".[dev]"`) | `scripts/setup_env.ps1`     |
| `.venv_dbc`  | 3.12   | Databricks Connect (serverless) for interactive Spark sessions against the workspace | `scripts/setup_env_dbc.ps1` |

`.venv_dbc` needs 3.12 specifically: the VS Code Databricks extension refuses to manage
Databricks Connect on 3.11, and `databricks-connect` is pinned to `16.1.7` in
`requirements-dbc.txt` because the latest release (19.x, the default on 3.12+) doesn't
support this workspace's serverless backend yet ("Serverless mode is not yet supported in
this version of Databricks Connect").

Both require the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) for
`databricks bundle ...` / `databricks auth login`.

```powershell
# .venv - ingestion + dbt
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# .venv_dbc - Databricks Connect (separate shell/activation)
.\.venv_dbc\Scripts\Activate.ps1
pip install -r requirements-dbc.txt
python scripts\dbc_connection_check.py   # sanity check the serverless session works

# configure secrets/paths (shared by both venvs)
copy .env.example .env   # then fill it in
```

### Run dbt locally

```powershell
cd dbt
dbt deps
dbt compile --profiles-dir .   # renders/parses all models, no warehouse writes
dbt build --profiles-dir .     # actually runs against the warehouse (includes the dim_ticker_snapshot SCD2 snapshot)
dbt docs generate --profiles-dir .   # builds catalog.json/manifest.json for the docs site
dbt docs serve                        # opens a local docs site with the DAG + column-level docs
```

### Validate / deploy the bundle

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run ingestion_daily_job -t dev
databricks bundle run ingestion_reference_job -t dev
databricks bundle run dbt_job -t dev
```

`dev` target already points at a real workspace/catalog/warehouse (see `databricks.yml`).
`test`/`prod` point at the same real workspace, different UC catalogs (`ygz_massive_stock_test`/
`ygz_massive_stock_prod`) - `-t test`/`-t prod` only work past `validate` once those catalogs
exist. No separate workspaces, no service principal - see
`plan/records/08_cicd_simplification_process.md`.

## Databricks resources

Created under the `DBT` profile in `~/.databrickscfg` (Azure workspace
`adb-7405607192769716.16.azuredatabricks.net`):

- Catalogs `ygz_massive_stock_dev`/`ygz_massive_stock_test`/`ygz_massive_stock_prod`, each with
  `landing` / `bronze` / `silver` / `gold` schemas (`dim_ticker_snapshot` also lives in `gold` -
  dbt snapshots don't get their own schema here) and a `landing.raw_massive_data` Volume
- SQL warehouse `2x Small serverless Warehouse` (id `04147fab6edc9014`) - what `dbt_task`/`dbt debug` connect through
- Secret scope `ygz-massive-stock`, holding `MASSIVE_API_KEY` - the ingestion jobs run on
  serverless compute, which has no cluster to attach `spark_env_vars`-style secret references to,
  so `ingestion/settings.py` reads it via `dbutils.secrets.get()` instead (falls back to the
  local `.env`/env var when `dbutils` isn't a real Databricks runtime, i.e. everywhere outside an
  actual job)

## CI/CD

Two workflows, each handling two trigger→action pairs via job-level `if: github.event_name`,
all gated by `pytest tests/` first:

| Workflow | Trigger | Action |
| --- | --- | --- |
| `ci-dev.yml` | push (any branch but `main`) | `bundle validate -t dev` (no deploy) |
| `ci-dev.yml` | pull request | `bundle validate`/`deploy -t dev` |
| `cd-test-prod.yml` | push to `main` | `bundle validate`/`deploy -t test` |
| `cd-test-prod.yml` | manual (`workflow_dispatch`) | `bundle validate`/`deploy -t prod` |

Deploying to prod is a deliberate action, not something that follows automatically from a push -
the `deploy-prod` job only runs on `workflow_dispatch`, so it only fires when someone goes to the
Actions tab and clicks "Run workflow". That sidesteps depending on GitHub Environment "required
reviewers" protection rules (not confirmed available on this repo's plan) for the same manual-gate
effect. (An earlier version split these into four single-trigger files - reverted after the
GitHub Actions sidebar's alphabetical-by-name ordering, not the trigger design, turned out to be
the real source of confusion - see `plan/records/12_cicd_consolidate_process.md`.)

Three environments, same Databricks workspace, different UC catalogs (no separate workspaces,
no service principal - see `plan/records/08_cicd_simplification_process.md`). None of the
workflows ever run the jobs themselves, just deploy their definitions - actual execution is
either the job's own schedule (dev's three jobs and test's `promote_from_dev_job`/`dbt_job` are
unpaused and run on cron - see Status/TODO) or a manual `bundle run` (prod stays fully manual,
deliberately - no schedule at all). Requires the repo secrets `DATABRICKS_HOST`/
`DATABRICKS_TOKEN` and the `ygz_massive_stock_test`/`ygz_massive_stock_prod` catalogs. See
`plan/records/10_cicd_trigger_refinement_process.md`/`11_job_scheduling_cost_process.md`/
`12_cicd_consolidate_process.md`.

## Status / TODO

- [x] Pick the market-data vendor (massive.com) and implement `ingestion/landing/client.py`
      against its real API
- [x] Land + Bronze-load all 5 sources (daily_bars, ticker_overview, splits, dividends, news)
      for the full watchlist
- [x] Align Silver dbt models with the real Bronze table shapes
- [x] Align Gold dbt models with the real Bronze table shapes, `dim_ticker` as SCD2
      via dbt snapshot (see `plan/design/02_data_model_design.md`)
- [x] Land all 5 sources daily (not just daily_bars) and make Bronze loads incremental
      via natural-key anti-join (see `ingestion/bronze/loader.py` module docstring)
- [x] Wire `MASSIVE_API_KEY` into the real job via a Databricks secret scope
      (`ygz-massive-stock`), and move both jobs to serverless compute instead of filling
      in cluster node types - `resources/clusters.yml` is gone, no `job_clusters` left in
      `resources/jobs.yml`. Config verified end-to-end (`bundle validate`/`deploy`/`run` all
      confirmed working, including a real `dbutils.secrets.get()` read)
- [x] Both `ingestion_daily_job` and `ingestion_reference_job` confirmed running end-to-end
      for real: `MassiveClient` now paces calls to massive.com's documented free-tier limit
      (5 requests/minute - see `ingestion/landing/client.py`), and `land_raw_json` was split
      into `land_daily_data` (daily_bars+news, needs a daily pull) and `land_reference_data`
      (ticker_overview/splits/dividends, doesn't - runs weekly, offset from the daily job's
      schedule so they can't overlap and jointly exceed the rate limit). Both real runs
      succeeded and grew Bronze row counts for real (see
      `plan/records/06_job_serverless_process.md`)
- [x] Gold's three derived-metric business rules are decided and implemented, not
      placeholders anymore: `fct_daily_returns.is_significant_move` (±3%),
      `fct_moving_averages.trend` (5d/20d moving-average crossover, 1% band for
      "sideways"), and the new `fct_volume_anomalies.is_volume_spike` (2x the trailing
      20-day average volume). Verified against real data, not just passing tests - see
      `plan/records/07_gold_business_rules_process.md`
- [x] CI/CD is live, two workflows gated by `pytest`: push validates dev, PR deploys dev,
      merge to main deploys test, `workflow_dispatch` (manual) deploys prod - see the CI/CD
      section above and `plan/records/10_cicd_trigger_refinement_process.md`/
      `12_cicd_consolidate_process.md`
- [x] `dev` is the only target that calls massive.com; `test`/`prod` get real data via
      `promote_from_dev_job` (copies dev's Landing Volume in, then runs `load_bronze` against
      that target's catalog). `ingestion_daily_job`/`ingestion_reference_job` are only defined
      under `targets.dev` in `databricks.yml` - not the shared `resources/jobs.yml` - so they
      structurally don't exist under `-t test`/`-t prod` at all; a documentation-only warning
      isn't a real guardrail against a job that's still deployed there. Also fixed a latent bug
      found along the way: `python_wheel_task`s never actually passed `${var.catalog}` through,
      so every real job run silently defaulted to the dev catalog regardless of target - see
      `plan/records/09_bronze_promotion_process.md`
- [x] Schedules are unpaused, tiered by how much each environment's freshness is worth: `dev`'s
      `ingestion_daily_job`/`ingestion_reference_job`/`dbt_job` run daily/weekly/daily for real
      (rough cost estimate ~$40-50/year on Azure serverless job compute pricing, ~$0.70-0.95/DBU-hour
      - see `plan/records/11_job_scheduling_cost_process.md`); `test`'s `promote_from_dev_job`/
      `dbt_job` run weekly (Monday, offset 30 min apart); `prod` stays fully manual, no schedule -
      deliberate, matching `cd-test-prod.yml`'s manual-confirmation deploy gate
- No real-time/Structured Streaming phase - dropped, not needed for this project
