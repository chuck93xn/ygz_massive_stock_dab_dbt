with daily_bars as (
    select * from {{ ref('fct_daily_bars') }}
),

with_moving_averages as (
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
)

-- Trend is a business decision (5d vs 20d moving-average crossover), not
-- derived from the data - see plan/requirements/requirement_breakdown.md.
-- The 1% tolerance band for "sideways" isn't one of the confirmed
-- thresholds; a plain crossover only gives up/down, but the requirement
-- calls for a third state when the two averages are basically on top of
-- each other, so this adds a band to catch that case.
select
    ticker,
    trade_date,
    close_price,
    moving_avg_5d,
    moving_avg_20d,
    moving_avg_50d,
    volatility_20d,
    case
        when moving_avg_20d is null then null
        when moving_avg_5d > moving_avg_20d * 1.01 then 'uptrend'
        when moving_avg_5d < moving_avg_20d * 0.99 then 'downtrend'
        else 'sideways'
    end as trend
from with_moving_averages
