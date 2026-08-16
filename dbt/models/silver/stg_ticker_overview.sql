with source as (
    select * from {{ source('bronze', 'ticker_overview') }}
),

renamed as (
    select
        ticker,
        name,
        market,
        primary_exchange,
        type,
        cast(active as boolean)  as active,
        currency_name,
        sic_code,
        sic_description,
        cast(market_cap as double)                          as market_cap,
        cast(share_class_shares_outstanding as bigint)       as share_class_shares_outstanding,
        cast(weighted_shares_outstanding as bigint)          as weighted_shares_outstanding,
        cast(total_employees as bigint)                      as total_employees,
        cast(list_date as date)                              as list_date,
        cast(_ingested_at as date) as snapshot_date,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    -- Dedup at (ticker, snapshot_date) grain rather than collapsing to one
    -- row per ticker - this preserves history so fct_ticker_daily_metrics
    -- can accumulate a real daily series once Landing for this source
    -- becomes incremental. Gold's dim_ticker is responsible for further
    -- collapsing this to "latest per ticker" (SCD1) for its own use.
    select *
    from renamed
    qualify row_number() over (
        partition by ticker, snapshot_date
        order by _ingested_at desc
    ) = 1
)

select * from deduped
