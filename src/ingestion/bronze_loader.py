"""Bronze layer: parse Landing JSON into structured, append-only Delta tables.

Column mapping for daily_bars matches massive.com's actual response shape -
verified against real landed data in
sketch/bronze_dev/daily_bars_bronze.ipynb. Bronze keeps these as fixed,
typed columns but doesn't dedup/clean/rename beyond that - that's Silver's
job in dbt.

This is a full-load: `load_daily_bars_to_bronze` reads everything under
`landing_path`, not just newly-landed files. Landing's own incrementality
(how far back an API pull goes) is a separate concern from Bronze's - see
plan/requirement_breakdown.md. Making this incremental (e.g. only reading
the latest date= partition, or Auto Loader) is a deliberately separate,
not-yet-built step; running this repeatedly with `mode("append")` today
would duplicate rows, so only call it when landing_path points at files
that haven't been loaded into `bronze_table` yet.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ingestion.settings import Settings

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
    "cast(_ingested_at as timestamp) as _ingested_at",
    # input_file_name() is not supported on Unity Catalog-governed compute
    # (UC_COMMAND_NOT_SUPPORTED) - _metadata.file_path is the replacement.
    "_metadata.file_path as _source_file",
]


def load_daily_bars_to_bronze(spark: SparkSession, *, landing_path: str, bronze_table: str) -> DataFrame:
    structured = spark.read.json(landing_path).selectExpr(*_DAILY_BARS_SELECT)
    (
        structured.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return structured


def main() -> None:
    """Entry point for the `load_bronze` DAB python_wheel_task.

    Full-load over the whole daily_bars Landing path - see module docstring
    on why this isn't incremental yet.
    """
    settings = Settings.from_env()
    spark = SparkSession.builder.getOrCreate()

    load_daily_bars_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/daily_bars",
        bronze_table=f"{settings.catalog}.{settings.bronze_schema}.daily_bars",
    )


if __name__ == "__main__":
    main()
