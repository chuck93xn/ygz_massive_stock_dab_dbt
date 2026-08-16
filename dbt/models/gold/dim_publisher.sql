with articles as (
    select * from {{ ref('stg_news_articles') }}
),

deduped as (
    select
        publisher_name,
        publisher_homepage_url,
        publisher_logo_url,
        publisher_favicon_url
    from articles
    where publisher_name is not null
    qualify row_number() over (
        partition by publisher_name
        order by published_utc desc
    ) = 1
)

select * from deduped
