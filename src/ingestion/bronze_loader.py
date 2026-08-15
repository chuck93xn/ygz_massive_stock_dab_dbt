"""Bronze layer: parse Landing JSON into a structured, append-only Delta table.

Schema is fixed here but values are intentionally not cleaned (no dedup, no
timezone normalization, no renaming) - that's Silver's job in dbt. This
module only adds audit columns so every Bronze row can be traced back to the
Landing file and ingestion timestamp it came from.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.settings import Settings


def load_landing_to_bronze(
    spark: SparkSession,
    *,
    landing_path: str,
    bronze_table: str,
) -> DataFrame:
    """Read newline-delimited JSON from `landing_path` and merge into `bronze_table`.

    TODO: once the vendor's actual response schema is known, replace the
    generic `record.*` flattening below with an explicit, typed select.
    """
    raw = spark.read.json(landing_path)

    structured = raw.select(
        "record.*",
        F.col("_ingested_at").cast("timestamp").alias("_ingested_at"),
        F.input_file_name().alias("_source_file"),
    )

    (
        structured.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return structured


def main() -> None:
    """Entry point for the `load_bronze` DAB python_wheel_task."""
    settings = Settings.from_env()
    spark = SparkSession.builder.getOrCreate()

    load_landing_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/tickers",
        bronze_table=f"{settings.catalog}.{settings.bronze_schema}.tickers",
    )


if __name__ == "__main__":
    main()
