# Massive Stock — End-to-End Data Engineering Platform

Stock market data pipeline built with **Databricks Asset Bundles (DAB)** and **dbt Core**.
<img width="1024" height="683" alt="8e7a6313-1fe9-482b-b2cc-cb087996fc08" src="https://github.com/user-attachments/assets/41e9075c-4524-4a7b-a01e-a1c042796097" />



## Architecture

massive.com REST API → Landing (raw JSON) → Bronze (structured Delta) → Silver/Gold (dbt Core).

| Layer       | Storage                          | Owner              | What happens here                                              |
| ----------- | --------------------------------- | ------------------ | ---------------------------------------------------------------- |
| **Landing** | Raw JSON in a UC Volume, append-only | Python (`src/ingestion/landing`) | Raw massive.com responses, partitioned by date, with request metadata |
| **Bronze**  | Delta table, fixed schema, unclean | Python/PySpark (`src/ingestion/bronze`, DAB job) | JSON parsed into columns + `_ingested_at`/`_source_file` audit fields |
| **Silver**  | Delta table, cleaned               | dbt Core            | UTC normalization, dedup, naming conventions, dbt tests          |
| **Gold**    | Delta table, analysis-ready        | dbt Core            | Daily returns, moving averages/trend, volume anomalies, ticker dimension |

- Watchlist is a fixed 10 tickers across all 5 massive.com endpoints (daily bars, ticker overview, splits, dividends, news).
- `land_daily_data` (daily bars + news) runs daily; `land_reference_data` (overview/splits/dividends, which the vendor always returns as full history) runs weekly, roughly halving the daily call volume.
- Bronze loads are idempotent: each `load_*_to_bronze` anti-joins against the table's natural key before appending, so re-running a load is always safe.
- `dev` is the only target that calls massive.com for real; `test`/`prod` get their data via `promote_from_dev_job`, which copies dev's Landing Volume and reruns `load_bronze` on top of it.
- `dim_ticker` is a proper SCD2 dimension via a dbt snapshot; Silver/Gold models are aligned with the real Bronze schema.

See [`docs/architecture.md`](docs/architecture.md) for the full design and
[`docs/engineering-log.md`](docs/engineering-log.md) for the decisions and real bugs behind it.

## Repo layout

```
docs/
  architecture.md      # system design
  engineering-log.md   # decisions, tradeoffs, real bugs
databricks.yml          # DAB bundle config (targets: dev/test/prod)
resources/jobs.yml      # dbt_job - shared across all 3 targets
src/ingestion/            # one subpackage per medallion layer: landing/, bronze/, promotion/
  settings.py              # env-var driven runtime config
tests/                    # unit tests, no real Spark/Databricks needed
scripts/                   # backfill, reload, and verification scripts
dbt/
  macros/generate_schema_name.sql    # custom schema naming (see engineering-log)
  models/silver/                     # staging models, aligned with Bronze
  models/gold/                       # dimensions + facts (see architecture.md#data-model)
  snapshots/dim_ticker_snapshot.sql  # SCD2 source for dim_ticker
.github/workflows/        # ci-dev.yml / cd-test.yml / cd-prod.yml (see CI/CD below)
```

## Local setup

Two separate virtual environments, on two different Python versions — `databricks-connect`
and `pyspark` can't coexist in the same environment, and Databricks Connect needs a newer
Python than the rest of the project:

| Venv         | Python | Purpose                                                        | Rebuild with                   |
| ------------ | ------ | ---------------------------------------------------------------- | ------------------------------- |
| `.venv`      | 3.11   | Ingestion unit tests, dbt Core, local pyspark logic (`pip install -e ".[dev]"`) | `scripts/setup_env.ps1`     |
| `.venv_dbc`  | 3.12   | Databricks Connect (serverless) for interactive Spark sessions against the workspace | `scripts/setup_env_dbc.ps1` |

`.venv_dbc` needs Python 3.12: the VS Code Databricks extension requires it, and
`databricks-connect` is pinned to `16.1.7` in `requirements-dbc.txt` because newer
releases (19.x+) don't support this workspace's serverless backend yet.

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

`dev` already points at a real workspace/catalog/warehouse. `test`/`prod` share the same
workspace, different UC catalogs (`ygz_massive_stock_test`/`ygz_massive_stock_prod`) -
`-t test`/`-t prod` only work past `validate` once those catalogs exist. No separate
workspaces, no service principal - see
[`docs/engineering-log.md`](docs/engineering-log.md#three-environment-isolation-is-structural-not-a-convention).

## Databricks resources

Created under the `DBT` profile in `~/.databrickscfg` (Azure workspace
`adb-7405607192769716.16.azuredatabricks.net`):

- Catalogs `ygz_massive_stock_dev`/`ygz_massive_stock_test`/`ygz_massive_stock_prod`, each with
  `landing` / `bronze` / `silver` / `gold` schemas (`dim_ticker_snapshot` also lives in `gold` -
  dbt snapshots don't get their own schema here) and a `landing.raw_massive_data` Volume
- SQL warehouse `2x Small serverless Warehouse` (id `04147fab6edc9014`) - what `dbt_task`/`dbt debug` connect through
- Secret scope `ygz-massive-stock`, holding `MASSIVE_API_KEY` - read via `dbutils.secrets.get()`
  (serverless compute has no cluster to attach `spark_env_vars` to); falls back to the local
  `.env` outside a real job

## CI/CD

Three workflows, each with exactly one trigger, all gated by `pytest tests/` first:

| Workflow | Trigger | Action |
| --- | --- | --- |
| `ci-dev.yml` | push (any branch but `main`) | `bundle validate -t dev` (no deploy) |
| `ci-dev.yml` | pull request | `bundle validate`/`deploy -t dev` |
| `cd-test.yml` | push to `main` | `bundle validate`/`deploy -t test` |
| `cd-prod.yml` | manual (`workflow_dispatch`) only | `bundle validate`/`deploy -t prod` |

Deploying to prod is a deliberate action, not something that follows automatically from a push -
`cd-prod.yml`'s only trigger is `workflow_dispatch`, fired manually from the Actions tab. The
3-file layout (rather than 2 or 4) came out of a few rounds of real workflow-ordering and re-run
failures, not an upfront design - see
[`docs/engineering-log.md`](docs/engineering-log.md#cicd-four-iterations-each-driven-by-a-real-failure)
for the full history.

None of the workflows run the jobs themselves, just deploy their definitions - actual execution is
either the job's own schedule (see [Status](#status)) or a manual `bundle run`. Requires the repo
secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN` and the `ygz_massive_stock_test`/`ygz_massive_stock_prod`
catalogs (see [Local setup](#local-setup)).

## Status

Feature-complete end to end, verified against real data and real Databricks runs, not just
passing tests:

- Full medallion pipeline (Landing → Bronze → Silver → Gold) live for all 5 massive.com sources across the 10-ticker watchlist, incrementally loaded and rate-limit aware.
- Silver/Gold dbt models finished, including `dim_ticker` as SCD2 and all three Gold business rules - significant moves, trend, volume spikes (see [`docs/architecture.md`](docs/architecture.md#gold-business-rules)).
- 3 isolated environments (dev/test/prod) on one workspace: `dev`-only real API calls, `promote_from_dev_job` for test/prod (see [`docs/engineering-log.md`](docs/engineering-log.md#three-environment-isolation-is-structural-not-a-convention)).
- CI/CD live: push validates dev, PR deploys dev, merge to main deploys test, manual dispatch deploys prod.
- Schedules unpaused and cost-tiered by environment - dev daily, test weekly, prod fully manual (see [`docs/engineering-log.md`](docs/engineering-log.md#cost-aware-scheduling)).

**Scope note:** no real-time/Structured Streaming phase - dropped, not needed for this project.
