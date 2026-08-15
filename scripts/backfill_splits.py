"""One-time (or rerunnable) load: land full split history for every
watchlist ticker into the real Landing Volume.

Usage (from .venv): python scripts/backfill_splits.py

No date range - get_splits() always pulls full history (see
plan/massive_client_design.md on why splits/dividends aren't "incremental").
Safe to rerun; Silver will need to dedup on replay.
"""

from functools import partial

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from ingestion.landing_writer import (
    WATCHLIST_TICKERS,
    land_splits,
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

    for ticker in WATCHLIST_TICKERS:
        path = land_splits(client, settings, ticker=ticker, writer=writer)
        print(f"{ticker}: landed to {path}")


if __name__ == "__main__":
    main()
