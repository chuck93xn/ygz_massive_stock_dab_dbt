"""DAB job entry point for the Landing layer (see pyproject.toml
[project.scripts] + resources/jobs.yml python_wheel_task). This is the only
module under ingestion/landing/ with a real entry point - client.py and
writer.py are pure functions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ingestion.landing.client import MassiveClient
from ingestion.landing.writer import WATCHLIST_TICKERS, land_daily_bars
from ingestion.settings import Settings


def land_raw_json() -> None:
    """Entry point for the `land_raw_json` DAB python_wheel_task.

    Daily incremental pull: a short recent window per watchlist ticker, so a
    normal run just tops up the last few days (Silver dedups any overlap).
    For the one-time full-history load, see scripts/landing/backfill_daily_bars.py.
    """
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)

    end = datetime.now(UTC).date()
    start = end - timedelta(days=5)

    for ticker in WATCHLIST_TICKERS:
        land_daily_bars(client, settings, ticker=ticker, start_date=start.isoformat(), end_date=end.isoformat())


if __name__ == "__main__":
    land_raw_json()
