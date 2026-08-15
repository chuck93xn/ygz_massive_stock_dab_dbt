"""Rerunnable full reload of all 6 Bronze tables from the real Landing data.

Usage (from .venv_dbc - needs a real Spark session, plain pyspark in .venv
can't reach Unity Catalog): python scripts/bronze/reload_bronze.py

`ingestion.bronze.job.load_bronze()` uses mode("append"), so calling it
repeatedly without dropping first would duplicate every row (Bronze isn't
incremental yet - see ingestion/bronze/loader.py's module docstring). This
script drops all 6 tables first, so each run leaves a clean, correct
snapshot instead of accumulating duplicates. That's a bigger hammer than a
real incremental design would need, but it's the right default for a
manual "reload everything from what's currently in Landing" tool.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from databricks.connect import DatabricksSession
from dotenv import load_dotenv

from ingestion.bronze.job import load_bronze

BRONZE_TABLES = [
    "daily_bars",
    "ticker_overview",
    "splits",
    "dividends",
    "news_articles",
    "news_sentiment",
]


def main() -> None:
    load_dotenv()

    # load_bronze() calls SparkSession.builder.getOrCreate(), which only
    # resolves an active session inside a real job/cluster. Pre-seeding a
    # DatabricksSession here makes getOrCreate() pick this one up instead,
    # so the same job.py code path works for both local runs and real jobs.
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

    catalog_schema = "ygz_massive_stock_dev.bronze"
    for table in BRONZE_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_schema}.{table}")

    load_bronze()

    for table in BRONZE_TABLES:
        count = spark.table(f"{catalog_schema}.{table}").count()
        print(f"{table}: {count} rows")


if __name__ == "__main__":
    main()
