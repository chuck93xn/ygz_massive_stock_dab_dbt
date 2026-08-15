"""DAB job entry point for the Bronze layer (see pyproject.toml
[project.scripts] + resources/jobs.yml python_wheel_task). This is the only
module under ingestion/bronze/ with a real entry point - loader.py is pure
functions.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from ingestion.bronze.loader import (
    load_daily_bars_to_bronze,
    load_dividends_to_bronze,
    load_news_articles_to_bronze,
    load_news_sentiment_to_bronze,
    load_splits_to_bronze,
    load_ticker_overview_to_bronze,
)
from ingestion.settings import Settings


def load_bronze() -> None:
    """Entry point for the `load_bronze` DAB python_wheel_task.

    Full-load over the whole Landing path for each source - see
    ingestion.bronze.loader's module docstring on why this isn't
    incremental yet.
    """
    settings = Settings.from_env()
    spark = SparkSession.builder.getOrCreate()
    catalog_schema = f"{settings.catalog}.{settings.bronze_schema}"

    load_daily_bars_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/daily_bars",
        bronze_table=f"{catalog_schema}.daily_bars",
    )
    load_ticker_overview_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/ticker_overview",
        bronze_table=f"{catalog_schema}.ticker_overview",
    )
    load_splits_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/splits",
        bronze_table=f"{catalog_schema}.splits",
    )
    load_dividends_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/dividends",
        bronze_table=f"{catalog_schema}.dividends",
    )
    load_news_articles_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/news",
        bronze_table=f"{catalog_schema}.news_articles",
    )
    load_news_sentiment_to_bronze(
        spark,
        landing_path=f"{settings.landing_volume_path}/news",
        bronze_table=f"{catalog_schema}.news_sentiment",
    )


if __name__ == "__main__":
    load_bronze()
