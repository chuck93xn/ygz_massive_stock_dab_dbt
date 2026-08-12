with source as (
    select * from {{ source('bronze', 'tickers') }}
),

renamed as (
    select
        ticker,
        name         as ticker_name,
        exchange,
        asset_type,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by ticker
        order by _ingested_at desc
    ) = 1
)

select * from deduped
