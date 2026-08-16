with daily_bars as (
    select * from {{ ref('fct_daily_bars') }}
)

select
    ticker,
    trade_date,
    close_price,
    avg(close_price) over (
        partition by ticker order by trade_date
        rows between 4 preceding and current row
    ) as moving_avg_5d,
    avg(close_price) over (
        partition by ticker order by trade_date
        rows between 19 preceding and current row
    ) as moving_avg_20d,
    avg(close_price) over (
        partition by ticker order by trade_date
        rows between 49 preceding and current row
    ) as moving_avg_50d,
    stddev(close_price) over (
        partition by ticker order by trade_date
        rows between 19 preceding and current row
    ) as volatility_20d
from daily_bars
