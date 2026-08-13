# ygz_massive_stock_dab_dbt

Stock market data pipeline built with **Databricks Asset Bundles (DAB)** and **dbt Core**.

## Architecture

**Phase 1** (current): vendor REST API → Landing (raw JSON) → Bronze (structured Delta) →
Silver/Gold (dbt Core).

**Phase 2** (planned): vendor WebSocket → Databricks Structured Streaming → real-time
aggregation tables.

| Layer       | Storage                          | Owner              | What happens here                                              |
| ----------- | --------------------------------- | ------------------ | ---------------------------------------------------------------- |
| **Landing** | Raw JSON in a UC Volume, append-only | Python (`src/ingestion`) | Raw vendor responses, partitioned by date, with request metadata |
| **Bronze**  | Delta table, fixed schema, unclean | Python/PySpark (DAB job) | JSON parsed into columns + `_ingested_at`/`_source_file` audit fields |
| **Silver**  | Delta table, cleaned               | dbt Core            | UTC normalization, dedup, naming conventions, dbt tests          |
| **Gold**    | Delta table, analysis-ready        | dbt Core            | Daily returns, moving averages, volatility, ticker dimension     |

> The market-data vendor itself hasn't been finalized yet (see `plan/introduction.md`,
> local-only). `src/ingestion/vendor_client.py` and the Bronze/Silver schemas below are
> placeholders with generic/neutral naming — expect a rename + real schema once the vendor
> is picked.

## Repo layout

```
databricks.yml              # DAB bundle root config (targets: dev/staging/prod)
resources/
  jobs.yml                  # ingestion_job (Landing+Bronze) and dbt_job (Silver+Gold)
  clusters.yml               # shared interactive dev cluster
src/ingestion/
  vendor_client.py           # REST client stub: auth, retry/backoff, pagination
  landing_writer.py           # writes raw JSON to the Landing Volume
  bronze_loader.py            # Landing -> Bronze (structured Delta)
  settings.py                 # env-var driven runtime config
dbt/
  dbt_project.yml
  profiles.yml                # local-dev only; DAB dbt_task auto-generates its own
  models/silver/               # stg_daily_bars, stg_tickers
  models/gold/                  # fct_daily_returns, fct_moving_averages, dim_ticker
.github/workflows/
  deploy-dev.yml               # auto-deploy on push to main
  deploy-test.yml              # deploy to staging on PR / manual dispatch
  deploy-prod.yml               # deploy to prod on release (needs environment approval)
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
dbt build --profiles-dir .     # actually runs against the warehouse
```

### Validate / deploy the bundle

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run ingestion_job -t dev
databricks bundle run dbt_job -t dev
```

`dev` target already points at a real workspace/catalog/warehouse (see `databricks.yml`).
`staging`/`prod` are still placeholders (`REPLACE_WITH_*`) until those environments exist.

## Databricks resources (dev)

Created under the `DBT` profile in `~/.databrickscfg` (Azure workspace
`adb-7405607192769716.16.azuredatabricks.net`):

- Catalog `ygz_massive_stock_dev`, with `landing` / `bronze` / `silver` / `gold` schemas
- Volume `ygz_massive_stock_dev.landing.raw` (Landing layer append target)
- SQL warehouse `2x Small serverless Warehouse` (id `04147fab6edc9014`) - what `dbt_task`/`dbt debug` connect through

## Status / TODO

- [ ] Pick the actual market-data vendor and rewrite `vendor_client.py` against its real API
- [ ] Rename neutral placeholders (`vendor_*`, generic table/column names) to match the chosen vendor
- [ ] Fill in cluster node types in `resources/jobs.yml` / `resources/clusters.yml`
- [ ] Create staging/prod catalogs + workspaces and fill in their `REPLACE_WITH_*` placeholders
- [ ] Phase 2: Structured Streaming job + real-time aggregation tables
