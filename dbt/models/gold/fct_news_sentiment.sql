-- Grain: article_id + ticker. Relates to dim_publisher via publisher_name
-- and dim_date via published_date (published_utc cast to date).
with sentiment as (
    select * from {{ ref('stg_news_sentiment') }}
),

articles as (
    select
        article_id,
        title,
        published_utc,
        publisher_name
    from {{ ref('stg_news_articles') }}
)

select
    sentiment.article_id,
    sentiment.ticker,
    articles.title,
    articles.published_utc,
    cast(articles.published_utc as date) as published_date,
    articles.publisher_name,
    sentiment.sentiment,
    sentiment.sentiment_reasoning
from sentiment
inner join articles on sentiment.article_id = articles.article_id
