with source as (
    select * from {{ source('bronze', 'daily_bars') }}
),

renamed as (
    select
        ticker,
        cast(trade_date as date)    as trade_date,
        cast(open_price as double)  as open_price,
        cast(high_price as double)  as high_price,
        cast(low_price as double)   as low_price,
        cast(close_price as double) as close_price,
        cast(volume as bigint)      as volume,
        -- assumes the vendor timestamps are UTC already; revisit once the
        -- real vendor's timezone convention is known.
        _ingested_at,
        _source_file
    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by ticker, trade_date
        order by _ingested_at desc
    ) = 1
)

select * from deduped
