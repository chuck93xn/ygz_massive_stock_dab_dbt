-- Known gap: plan/data_model_design.md's dim_publisher design also calls
-- for favicon_url, but src/ingestion/bronze/loader.py's _NEWS_ARTICLES_SELECT
-- doesn't capture it from the source API response. Deferred to a future
-- Bronze-focused task rather than expanding scope here.
with articles as (
    select * from {{ ref('stg_news_articles') }}
),

deduped as (
    select
        publisher_name,
        publisher_homepage_url,
        publisher_logo_url
    from articles
    where publisher_name is not null
    qualify row_number() over (
        partition by publisher_name
        order by published_utc desc
    ) = 1
)

select * from deduped
