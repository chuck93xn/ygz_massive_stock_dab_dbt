"""One-time backfill: land ~1 year of daily bars for every watchlist ticker
into the real Landing Volume.

Usage (from .venv): python scripts/backfill_daily_bars.py

Writes via the Databricks Files API (not the FUSE-mount path
landing_writer.write_landing_records assumes - that only works inside an
actual Databricks cluster/job). Re-running this is safe: each run lands a
new dated/uuid'd file, and Silver's dedup logic (stg_daily_bars.sql) already
collapses overlapping ticker+trade_date rows to the latest _ingested_at.

If this hits massive.com's rate limit (429), it'll wait it out and retry
automatically (see massive_client.py) - with 10 tickers this can trigger a
few waits, expect it to take a few minutes, not a hang.
"""

from datetime import UTC, datetime, timedelta
from functools import partial

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from ingestion.landing_writer import (
    WATCHLIST_TICKERS,
    land_daily_bars,
    write_landing_records_via_files_api,
)
from ingestion.massive_client import MassiveClient
from ingestion.settings import Settings


def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)
    ws = WorkspaceClient(profile="DBT")
    writer = partial(write_landing_records_via_files_api, workspace_client=ws)

    end = datetime.now(UTC).date()
    start = end - timedelta(days=365)

    for ticker in WATCHLIST_TICKERS:
        path = land_daily_bars(
            client,
            settings,
            ticker=ticker,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            writer=writer,
        )
        print(f"{ticker}: landed to {path}")


if __name__ == "__main__":
    main()
