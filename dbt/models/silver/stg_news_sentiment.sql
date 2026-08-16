with source as (
    select * from {{ source('bronze', 'news_sentiment') }}
),

renamed as (
    select
        article_id,
        ticker,
        sentiment,
        sentiment_reasoning,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    -- Bronze already dedups on (article_id, ticker); this re-guarantees it
    -- at Silver per the project's usual convention.
    select *
    from renamed
    qualify row_number() over (
        partition by article_id, ticker
        order by _ingested_at desc
    ) = 1
)

select * from deduped
