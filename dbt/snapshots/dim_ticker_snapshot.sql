{% snapshot dim_ticker_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='ticker',
        strategy='check',
        check_cols=['name', 'primary_exchange', 'sic_code', 'type', 'active', 'currency_name'],
    )
}}

with ticker_overview as (
    select * from {{ ref('stg_ticker_overview') }}
),

latest as (
    -- snapshot 的 select 必须是"当前状态、每个 unique_key 一行" - 历史版本
    -- 由 snapshot 机制自己维护,不是这里的 select 该做的事.
    select *
    from ticker_overview
    qualify row_number() over (
        partition by ticker
        order by snapshot_date desc, _ingested_at desc
    ) = 1
)

select
    ticker,
    name,
    primary_exchange,
    sic_code,
    type,
    active,
    currency_name
from latest

{% endsnapshot %}
