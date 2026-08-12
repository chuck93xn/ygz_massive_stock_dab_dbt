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

Requires Python 3.10/3.11 (matches Databricks Runtime + `dbt-databricks`; a `.venv` built
with 3.11 is already set up at repo root) and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).

```powershell
# activate the venv
.\.venv\Scripts\Activate.ps1

# install ingestion + dbt deps
pip install -e ".[dev]"

# configure secrets/paths
copy .env.example .env   # then fill it in
```

### Run dbt locally

```powershell
cd dbt
dbt deps
dbt build --profiles-dir .
```

### Validate / deploy the bundle

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run ingestion_job -t dev
databricks bundle run dbt_job -t dev
```

Fill in the `REPLACE_WITH_*` placeholders in `databricks.yml` and `resources/*.yml`
(workspace hosts, node type, SQL warehouse id, prod service principal) before deploying.

## Status / TODO

- [ ] Pick the actual market-data vendor and rewrite `vendor_client.py` against its real API
- [ ] Rename neutral placeholders (`vendor_*`, generic table/column names) to match the chosen vendor
- [ ] Fill in cluster node types + SQL warehouse id in `databricks.yml`
- [ ] Phase 2: Structured Streaming job + real-time aggregation tables
