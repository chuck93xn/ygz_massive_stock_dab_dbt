with source as (
    select * from {{ source('bronze', 'dividends') }}
),

renamed as (
    select
        dividend_id,
        ticker,
        cast(ex_dividend_date as date)  as ex_dividend_date,
        cast(declaration_date as date)  as declaration_date,
        cast(record_date as date)       as record_date,
        cast(pay_date as date)          as pay_date,
        cast(cash_amount as double)     as cash_amount,
        currency,
        frequency,
        distribution_type,
        cast(historical_adjustment_factor as double) as historical_adjustment_factor,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by dividend_id
        order by _ingested_at desc
    ) = 1
)

select * from deduped
