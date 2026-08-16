-- Grain: ticker + snapshot_date. Currently a near-single-row-per-ticker
-- table in practice, since Landing only manually backfills ticker_overview
-- (not a daily job yet) - see plan/data_model_design.md. This will
-- naturally accumulate into a real daily series once that Landing source
-- becomes incremental; no changes needed here when that happens.
select
    ticker,
    snapshot_date,
    market_cap,
    share_class_shares_outstanding,
    weighted_shares_outstanding
from {{ ref('stg_ticker_overview') }}
