select
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    transaction_count
from {{ ref('stg_daily_bars') }}
