-- Grain: ticker + snapshot_date - see plan/design/02_data_model_design.md.
-- ticker_overview now lands weekly via land_reference_data() (not daily -
-- see plan/records/06_job_serverless_process.md on why), so this
-- accumulates one new row per ticker per real run, not truly daily; no
-- changes needed here if that cadence ever changes.
select
    ticker,
    snapshot_date,
    market_cap,
    share_class_shares_outstanding,
    weighted_shares_outstanding
from {{ ref('stg_ticker_overview') }}
