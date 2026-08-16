with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min(trade_date) from " ~ ref('stg_daily_bars') ~ ")",
        end_date="dateadd(day, 30, (select max(trade_date) from " ~ ref('stg_daily_bars') ~ "))"
    ) }}
)

select
    cast(date_day as date) as date_day,
    dayofweek(date_day)    as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    month(date_day)        as month,
    quarter(date_day)      as quarter,
    year(date_day)         as year,
    dayofweek(date_day) in (1, 7) as is_weekend,
    -- simplified: "trading day" = not a weekend. Doesn't yet account for
    -- market holidays.
    not (dayofweek(date_day) in (1, 7)) as is_trading_day
from spine
