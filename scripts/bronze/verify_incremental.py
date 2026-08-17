"""Rerunnable regression check: calling load_bronze() twice back-to-back
(no Landing data lands in between, since this script runs both calls
itself) must produce identical row counts in all 6 Bronze tables. If any
table grows, the natural-key anti-join in ingestion/bronze/loader.py has
regressed - see plan/records/04_bronze_landing_incremental_process.md for why
this property matters (an earlier design silently duplicated every row on
rerun).

Usage (from .venv_dbc - needs a real Spark session, plain pyspark in .venv
can't reach Unity Catalog): python scripts/bronze/verify_incremental.py

Safe to run anytime against the real dev catalog - it never drops data,
only appends whatever's still new (same as a normal load_bronze() call).
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
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    catalog_schema = "ygz_massive_stock_dev.bronze"

    load_bronze()
    counts_1 = {t: spark.table(f"{catalog_schema}.{t}").count() for t in BRONZE_TABLES}

    load_bronze()
    counts_2 = {t: spark.table(f"{catalog_schema}.{t}").count() for t in BRONZE_TABLES}

    print(f"{'table':<20}{'run 1':>8}{'run 2':>8}  status")
    any_failed = False
    for table in BRONZE_TABLES:
        before, after = counts_1[table], counts_2[table]
        ok = before == after
        any_failed = any_failed or not ok
        status = "OK" if ok else "FAIL - DUPLICATED"
        print(f"{table:<20}{before:>8}{after:>8}  {status}")

    if any_failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
