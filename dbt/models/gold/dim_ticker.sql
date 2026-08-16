select
    ticker,
    ticker_name,
    exchange,
    asset_type
from {{ ref('stg_ticker_overview') }}
