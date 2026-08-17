with daily_bars as (
    select * from {{ ref('fct_daily_bars') }}
),

with_prev_close as (
    select
        ticker,
        trade_date,
        close_price,
        lag(close_price) over (
            partition by ticker
            order by trade_date
        ) as prev_close_price
    from daily_bars
),

with_return as (
    select
        ticker,
        trade_date,
        close_price,
        prev_close_price,
        case
            when prev_close_price is not null and prev_close_price != 0
                then (close_price - prev_close_price) / prev_close_price
        end as daily_return
    from with_prev_close
)

-- "Significant move" threshold (+/-3%) is a business decision, not derived
-- from the data - see plan/requirements/requirement_breakdown.md.
select
    ticker,
    trade_date,
    close_price,
    prev_close_price,
    daily_return,
    case
        when daily_return is not null
            then abs(daily_return) >= 0.03
    end as is_significant_move
from with_return
