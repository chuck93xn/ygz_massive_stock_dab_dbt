with source as (
    select * from {{ source('bronze', 'splits') }}
),

renamed as (
    select
        split_id,
        ticker,
        cast(execution_date as date) as execution_date,
        cast(split_from as double) as split_from,
        cast(split_to as double)   as split_to,
        adjustment_type,
        cast(historical_adjustment_factor as double) as historical_adjustment_factor,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by split_id
        order by _ingested_at desc
    ) = 1
)

select * from deduped
