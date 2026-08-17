with daily_bars as (
    select * from {{ ref('fct_daily_bars') }}
),

-- avg_volume_20d deliberately excludes trade_date itself (unlike
-- fct_moving_averages' price windows, which include the current row) - a
-- volume spike would otherwise inflate its own 20-day baseline and mute
-- the ratio that's supposed to catch it.
with_avg_volume as (
    select
        ticker,
        trade_date,
        volume,
        avg(volume) over (
            partition by ticker order by trade_date
            rows between 20 preceding and 1 preceding
        ) as avg_volume_20d
    from daily_bars
)

-- Volume-spike threshold (2x the 20-day average) is a business decision,
-- not derived from the data - see plan/requirements/requirement_breakdown.md.
select
    ticker,
    trade_date,
    volume,
    avg_volume_20d,
    case
        when avg_volume_20d is not null and avg_volume_20d != 0
            then volume / avg_volume_20d
    end as volume_ratio,
    case
        when avg_volume_20d is not null and avg_volume_20d != 0
            then volume >= 2 * avg_volume_20d
    end as is_volume_spike
from with_avg_volume
