"""One-time (or rerunnable) load: land the current Ticker Overview snapshot
for every watchlist ticker into the real Landing Volume.

Usage (from .venv): python scripts/backfill_ticker_overview.py

Unlike daily_bars, there's no date range - Ticker Overview is a single
current-state snapshot per ticker, not a time series. Safe to rerun
anytime you want a fresher snapshot (e.g. market_cap changes daily).

Writes via the Databricks Files API (see scripts/backfill_daily_bars.py for
why - /Volumes/... isn't FUSE-mounted outside a cluster/job).
"""

from functools import partial

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from ingestion.landing_writer import (
    WATCHLIST_TICKERS,
    land_ticker_overview,
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
        path = land_ticker_overview(client, settings, ticker=ticker, writer=writer)
        print(f"{ticker}: landed to {path}")


if __name__ == "__main__":
    main()
