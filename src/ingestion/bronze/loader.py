"""Bronze layer: parse Landing JSON into structured, append-only Delta tables.

Column mappings match massive.com's actual response shapes - verified
against real landed data in sketch/bronze_dev/*.ipynb. Bronze keeps these
as fixed, typed columns but doesn't dedup/clean/rename beyond that - that's
Silver's job in dbt.

Incremental by natural-key anti-join, not by limiting what's read: each
`load_*_to_bronze` still reads the full Landing history for its source
every run (cheap at this project's 10-ticker scale), then anti-joins out
any row whose natural key (e.g. (ticker, trade_date) for daily_bars,
split_id for splits) already exists in `bronze_table` before appending.

An earlier version of this tried to prune by the `date=` Landing partition
instead (only read partitions newer than a watermark). That looked right
but was wrong: massive.com's splits/dividends/ticker_overview endpoints
replay full history on every call (no date filter), so a new day's
partition isn't incremental data, it's the same history again - "only read
new partitions" ended up re-appending the entire table's history on every
run instead of preventing duplicates. Anti-joining on the natural key is
correct regardless of whether a source's Landing payload is a true delta
or a full replay, which is why it's used everywhere here instead.

First run against a table that doesn't exist yet has nothing to anti-join
against, so it reads and inserts everything - same cold-start behavior as
before.
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
    "record.publisher.favicon_url as publisher_favicon_url",
    *_AUDIT_COLUMNS,
]


def _exclude_existing_keys(
    spark: SparkSession,
    structured: DataFrame,
    bronze_table: str,
    key_cols: list[str],
) -> DataFrame:
    """Anti-join out any row whose key_cols already exist in bronze_table,
    so re-running a load doesn't insert duplicates - regardless of whether
    the Landing payload it read was genuinely new data or a full replay of
    history. No-op (reads/writes everything) on the very first run, when
    bronze_table doesn't exist yet."""
    if not spark.catalog.tableExists(bronze_table):
        return structured
    existing_keys = spark.table(bronze_table).select(*key_cols).distinct()
    return structured.join(existing_keys, on=key_cols, how="left_anti")


def _load(
    spark: SparkSession,
    landing_path: str,
    bronze_table: str,
    select_exprs: list[str],
    *,
    dedup_subset: list[str] | None = None,
    key_cols: list[str] | None = None,
) -> DataFrame:
    structured = spark.read.json(landing_path).selectExpr(*select_exprs)
    if dedup_subset:
        structured = structured.dropDuplicates(dedup_subset)
    if key_cols:
        structured = _exclude_existing_keys(spark, structured, bronze_table, key_cols)
    (
        structured.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return structured


def load_daily_bars_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _DAILY_BARS_SELECT, key_cols=["ticker", "trade_date"])


def load_ticker_overview_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    """Bronze deliberately keeps one row per (ticker, calendar day landed) -
    not one row per ticker - so fct_ticker_daily_metrics can accumulate real
    daily history (see plan/design/data_model_design.md). The API response
    has no date field of its own, so the idempotency key derives a day from
    _ingested_at on both sides of the anti-join; that derived column is
    never persisted (Silver's stg_ticker_overview.sql already derives the
    same thing as snapshot_date, so Bronze doesn't need its own copy)."""
    structured = spark.read.json(landing_path).selectExpr(*_TICKER_OVERVIEW_SELECT)
    if spark.catalog.tableExists(bronze_table):
        keyed = structured.selectExpr("*", "cast(_ingested_at as date) as _ingested_date")
        existing_keys = (
            spark.table(bronze_table)
            .selectExpr("ticker", "cast(_ingested_at as date) as _ingested_date")
            .distinct()
        )
        structured = keyed.join(existing_keys, on=["ticker", "_ingested_date"], how="left_anti").drop(
            "_ingested_date"
        )
    (
        structured.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return structured


def load_splits_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _SPLITS_SELECT, key_cols=["split_id"])


def load_dividends_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    return _load(spark, landing_path, bronze_table, _DIVIDENDS_SELECT, key_cols=["dividend_id"])


def load_news_articles_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    """One row per article. The same article can be landed multiple times
    (once per watchlist ticker whose query happened to return it, and again
    on subsequent days since the vendor's recent-news window overlaps run
    to run), so this dedups on article_id both within a single batch
    (dedup_subset) and across runs (key_cols)."""
    return _load(
        spark,
        landing_path,
        bronze_table,
        _NEWS_ARTICLES_SELECT,
        dedup_subset=["article_id"],
        key_cols=["article_id"],
    )


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
    sentiment = _exclude_existing_keys(spark, sentiment, bronze_table, ["article_id", "ticker"])
    (
        sentiment.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return sentiment
