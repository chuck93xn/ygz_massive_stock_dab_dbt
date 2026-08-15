"""One-time (or rerunnable) load: land recent news for every watchlist
ticker into the real Landing Volume.

Usage (from .venv): python scripts/landing/backfill_news.py

Uses get_news()'s defaults (last 7 days, capped at 5 articles/ticker - the
project's decision, see plan/requirement_breakdown.md). Meant to be rerun
regularly (news goes stale), not a true one-time backfill like the others.
"""

from functools import partial

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from ingestion.landing.client import MassiveClient
from ingestion.landing.writer import (
    WATCHLIST_TICKERS,
    land_news,
    write_landing_records_via_files_api,
)
from ingestion.settings import Settings


def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)
    ws = WorkspaceClient(profile="DBT")
    writer = partial(write_landing_records_via_files_api, workspace_client=ws)

    for ticker in WATCHLIST_TICKERS:
        path = land_news(client, settings, ticker=ticker, writer=writer)
        print(f"{ticker}: landed to {path}")


if __name__ == "__main__":
    main()
