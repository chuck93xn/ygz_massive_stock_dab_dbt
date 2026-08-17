# ygz_massive_stock_dab_dbt

Stock market data pipeline built with **Databricks Asset Bundles (DAB)** and **dbt Core**.

## Architecture

**Phase 1** (current): massive.com REST API → Landing (raw JSON) → Bronze (structured Delta)
→ Silver/Gold (dbt Core).

**Phase 2** (planned): massive.com WebSocket → Databricks Structured Streaming → real-time
aggregation tables.

| Layer       | Storage                          | Owner              | What happens here                                              |
| ----------- | --------------------------------- | ------------------ | ---------------------------------------------------------------- |
| **Landing** | Raw JSON in a UC Volume, append-only | Python (`src/ingestion/landing`) | Raw massive.com responses, partitioned by date, with request metadata |
| **Bronze**  | Delta table, fixed schema, unclean | Python/PySpark (`src/ingestion/bronze`, DAB job) | JSON parsed into columns + `_ingested_at`/`_source_file` audit fields |
| **Silver**  | Delta table, cleaned               | dbt Core            | UTC normalization, dedup, naming conventions, dbt tests          |
| **Gold**    | Delta table, analysis-ready        | dbt Core            | Daily returns, moving averages, volatility, ticker dimension     |

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

## Repo layout

`src/ingestion/` splits into one subpackage per medallion layer. Each layer has pure
functions in its own module, and - only where a real DAB job needs to call into it - a thin
`job.py` with just the entry point (imports the real logic, no logic of its own). This is
the only place `main()`-shaped code lives; everything else is importable functions.

```
databricks.yml              # DAB bundle root config (targets: dev/staging/prod)
resources/
  jobs.yml                  # ingestion_daily_job, ingestion_reference_job (Landing+Bronze,
                             #   different schedules), dbt_job (Silver+Gold) - all serverless
src/ingestion/
  settings.py                 # env-var driven runtime config, shared across layers
  landing/
    client.py                   # MassiveClient: auth, retry/backoff, pagination, inter-request delay
    writer.py                    # land_*() + write_landing_records*() - pure functions
    job.py                         # land_daily_data()/land_reference_data() - DAB job entry points
  bronze/
    loader.py                    # load_*_to_bronze() - pure functions
    job.py                         # load_bronze() - the load_bronze DAB job entry point
tests/
  test_massive_client.py          # MassiveClient logic against a fake session
  test_bronze_loader.py            # _exclude_existing_keys bootstrap case (no real Spark locally)
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
                                #   fct_moving_averages - aligned with Bronze
  snapshots/
    dim_ticker_snapshot.sql      # SCD2 source for dim_ticker (strategy=check)
  tests/
    assert_dim_ticker_single_current_version.sql   # singular test: SCD2 invariant
.github/workflows/
  test.yml                     # pytest on every PR/push to main - active, no secrets needed
.github/workflows-disabled/
  deploy-dev.yml               # auto-deploy on push to main (disabled, see below)
  deploy-test.yml              # deploy to staging on PR / manual dispatch (disabled)
  deploy-prod.yml               # deploy to prod on release (disabled)
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
`staging`/`prod` are still placeholders (`REPLACE_WITH_*`) until those environments exist.

## Databricks resources (dev)

Created under the `DBT` profile in `~/.databrickscfg` (Azure workspace
`adb-7405607192769716.16.azuredatabricks.net`):

- Catalog `ygz_massive_stock_dev`, with `landing` / `bronze` / `silver` / `gold` schemas
  (`dim_ticker_snapshot` also lives in `gold` - dbt snapshots don't get their own schema here)
- Volume `ygz_massive_stock_dev.landing.raw` (Landing layer append target)
- SQL warehouse `2x Small serverless Warehouse` (id `04147fab6edc9014`) - what `dbt_task`/`dbt debug` connect through
- Secret scope `ygz-massive-stock`, holding `MASSIVE_API_KEY` - both jobs in `resources/jobs.yml`
  run on serverless compute, which has no cluster to attach `spark_env_vars`-style secret
  references to, so `ingestion/settings.py` reads it via `dbutils.secrets.get()` instead (falls
  back to the local `.env`/env var when `dbutils` isn't a real Databricks runtime, i.e. everywhere
  outside an actual job)

## CI/CD

`.github/workflows/test.yml` runs `pytest tests/` on every PR and push to
`main` - plain Python, no Databricks connection or secrets needed, so it's
active today.

The *deploy* workflows are a separate thing and live in
`.github/workflows-disabled/`, not `.github/workflows/` — GitHub Actions
only scans the latter, so they're inert. They were failing on every push
(no `DATABRICKS_HOST_*`/`TOKEN_*` secrets configured, and `staging`/`prod`
targets are still placeholders), so they're parked until the project is far
enough along to actually deploy. See `.github/workflows-disabled/README.md`
for how to re-enable.

## Status / TODO

- [x] Pick the market-data vendor (massive.com) and implement `ingestion/landing/client.py`
      against its real API
- [x] Land + Bronze-load all 5 sources (daily_bars, ticker_overview, splits, dividends, news)
      for the full watchlist
- [x] Align Silver dbt models with the real Bronze table shapes
- [x] Align Gold dbt models with the real Bronze table shapes, `dim_ticker` as SCD2
      via dbt snapshot (see `plan/design/data_model_design.md`)
- [x] Land all 5 sources daily (not just daily_bars) and make Bronze loads incremental
      via natural-key anti-join (see `ingestion/bronze/loader.py` module docstring)
- [x] Add a PR-triggered `pytest` CI gate (`.github/workflows/test.yml`) - separate from
      the deploy workflows below, needs no Databricks connection or secrets
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
- [ ] Create staging/prod catalogs + workspaces and fill in their `REPLACE_WITH_*` placeholders
- [ ] Phase 2: Structured Streaming job + real-time aggregation tables
