with source as (
    select * from {{ source('bronze', 'news_articles') }}
),

renamed as (
    select
        article_id,
        title,
        description,
        article_url,
        published_utc,
        publisher_name,
        publisher_homepage_url,
        publisher_logo_url,
        _ingested_at,
        _source_file
    from source
),

deduped as (
    -- Bronze already dedups on article_id; this re-guarantees it at Silver
    -- per the project's usual convention.
    select *
    from renamed
    qualify row_number() over (
        partition by article_id
        order by _ingested_at desc
    ) = 1
)

select * from deduped
