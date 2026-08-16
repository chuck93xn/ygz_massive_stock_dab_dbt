with ticker_overview as (
    select * from {{ ref('stg_ticker_overview') }}
),

deduped as (
    select
        sic_code,
        sic_description
    from ticker_overview
    where sic_code is not null
    qualify row_number() over (
        partition by sic_code
        order by snapshot_date desc
    ) = 1
)

select * from deduped
