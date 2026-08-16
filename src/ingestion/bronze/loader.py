"""Bronze layer: parse Landing JSON into structured, append-only Delta tables.

Column mappings match massive.com's actual response shapes - verified
against real landed data in sketch/bronze_dev/*.ipynb. Bronze keeps these
as fixed, typed columns but doesn't dedup/clean/rename beyond that - that's
Silver's job in dbt.

These are full-loads: each `load_*_to_bronze` reads everything under
`landing_path`, not just newly-landed files. Landing's own incrementality
(how far back an API pull goes) is a separate concern from Bronze's - see
plan/requirements/requirement_breakdown.md. Making this incremental (e.g. only reading
the latest date= partition, or Auto Loader) is a deliberately separate,
not-yet-built step; running these repeatedly with `mode("append")` today
would duplicate rows, so only call them when landing_path points at files
that haven't been loaded into `bronze_table` yet.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

_AUDIT_COLUMNS = [
    "cast(_ingested_at as timestamp) as _ingested_at",
    # input_file_name() is not supported on Unity Catalog-governed compute
    # (UC_COMMAND_NOT_SUPPORTED) - _metadata.file_path is the replacement.
    "_metadata.file_path as _source_file",
]

# record.t is epoch ms; ticker isn't in the daily-bar record itself (the
# endpoint is scoped to one ticker via the URL), so it comes from the
# request metadata landing_writer.land_daily_bars() stores alongside each bar.
_DAILY_BARS_SELECT = [
    "_request_metadata.ticker as ticker",
    "cast(from_unixtime(record.t / 1000) as date) as trade_date",
    "record.o as open_price",
    "record.h as high_price",
    "record.l as low_price",
    "record.c as close_price",
    "record.v as volume",
    "record.vw as vwap",
    "record.n as transaction_count",
    *_AUDIT_COLUMNS,
]

# Unlike daily_bars, the ticker_overview record carries its own `ticker`
# field. Only the columns plan/design/data_model_design.md's dim_ticker /
# fct_ticker_daily_metrics design actually needs are selected here - the
# response has more (address, branding, phone_number, description, ...)
# that nothing downstream uses yet.
_TICKER_OVERVIEW_SELECT = [
    "record.ticker as ticker",
    "record.name as name",
    "record.market as market",
    "record.primary_exchange as primary_exchange",
    "record.type as type",
    "record.active as active",
    "record.currency_name as currency_name",
    "record.sic_code as sic_code",
    "record.sic_description as sic_description",
    "record.market_cap as market_cap",
    "record.share_class_shares_outstanding as share_class_shares_outstanding",
    "record.weighted_shares_outstanding as weighted_shares_outstanding",
    "record.total_employees as total_employees",
    "record.list_date as list_date",
    *_AUDIT_COLUMNS,
]


# Sparse, full-history event records - ticker is in the response itself.
_SPLITS_SELECT = [
    "record.id as split_id",
    "record.ticker as ticker",
    "cast(record.execution_date as date) as execution_date",
    "record.split_from as split_from",
    "record.split_to as split_to",
    "record.adjustment_type as adjustment_type",
    "record.historical_adjustment_factor as historical_adjustment_factor",
    *_AUDIT_COLUMNS,
]

_DIVIDENDS_SELECT = [
    "record.id as dividend_id",
    "record.ticker as ticker",
    "cast(record.ex_dividend_date as date) as ex_dividend_date",
    "cast(record.declaration_date as date) as declaration_date",
    "cast(record.record_date as date) as record_date",
    "cast(record.pay_date as date) as pay_date",
    "record.cash_amount as cash_amount",
    "record.currency as currency",
    "record.frequency as frequency",
    "record.distribution_type as distribution_type",
    "record.historical_adjustment_factor as historical_adjustment_factor",
    *_AUDIT_COLUMNS,
]

_NEWS_ARTICLES_SELECT = [
    "record.id as article_id",
    "record.title as title",
    "record.description as description",
    "record.article_url as article_url",
    "cast(record.published_utc as timestamp) as published_utc",
    "record.publisher.name as publisher_name",
    "record.publisher.homepage_url as publisher_homepage_url",
    "record.publisher.logo_url as publisher_logo_url",
    *_AUDIT_COLUMNS,
]


def _load(
    spark: SparkSession,
    landing_path: str,
    bronze_table: str,
    select_exprs: list[str],
    *,
    dedup_subset: list[str] | None = None,
) -> DataFrame:
    structured = spark.read.json(landing_path).selectExpr(*select_exprs)
    if dedup_subset:
        structured = structured.dropDuplicates(dedup_subset)
    (
        structured.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return structured


def load_daily_bars_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _DAILY_BARS_SELECT)


def load_ticker_overview_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _TICKER_OVERVIEW_SELECT)


def load_splits_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _SPLITS_SELECT)


def load_dividends_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _DIVIDENDS_SELECT)


def load_news_articles_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    """One row per article. The same article can be landed multiple times
    (once per watchlist ticker whose query happened to return it), so this
    dedups on article_id - unlike the other sources, re-running this with
    mode("append") would otherwise accumulate duplicate article rows even
    within a single load, not just across repeated runs."""
    return _load(spark, landing_path, bronze_table, _NEWS_ARTICLES_SELECT, dedup_subset=["article_id"])


def load_news_sentiment_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    """One row per (article, ticker) - explodes each article's `insights`
    array, which covers every ticker the article mentions, not just
    watchlist tickers (ETF holdings, preferred share classes, etc. show up
    here too; Bronze doesn't filter them out)."""
    sentiment = (
        spark.read.json(landing_path)
        .selectExpr("record.id as article_id", "explode(record.insights) as insight", *_AUDIT_COLUMNS)
        .selectExpr(
            "article_id",
            "insight.ticker as ticker",
            "insight.sentiment as sentiment",
            "insight.sentiment_reasoning as sentiment_reasoning",
            "_ingested_at",
            "_source_file",
        )
        .dropDuplicates(["article_id", "ticker"])
    )
    (
        sentiment.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return sentiment
