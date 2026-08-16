"""DAB job entry point for the Landing layer (see pyproject.toml
[project.scripts] + resources/jobs.yml python_wheel_task). This is the only
module under ingestion/landing/ with a real entry point - client.py and
writer.py are pure functions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ingestion.landing.client import MassiveClient
from ingestion.landing.writer import (
    WATCHLIST_TICKERS,
    land_daily_bars,
    land_dividends,
    land_news,
    land_splits,
    land_ticker_overview,
)
from ingestion.settings import Settings


def land_raw_json() -> None:
    """Entry point for the `land_raw_json` DAB python_wheel_task.

    Daily pull for all 5 sources, per watchlist ticker:
    - daily_bars: a short recent window (Silver dedups any overlap). For the
      one-time full-history load, see scripts/landing/backfill_daily_bars.py.
    - ticker_overview: today's snapshot - no window, always "current state".
      This is also what feeds Gold's dim_ticker_snapshot SCD2 detection, so
      landing it daily (not just once via the backfill script) is what
      actually lets that snapshot pick up real-world changes over time.
    - splits/dividends: the vendor endpoints return full history on every
      call (no date param), so a daily pull is just a cheap re-snapshot, not
      a windowed increment.
    - news: MassiveClient.get_news's own default window (last 7 days, top 5
      per ticker - see plan/requirements/requirement_breakdown.md).

    Any single API call raising propagates and fails the whole job - same
    fail-fast behavior as before, no new retry/skip logic here (retries are
    already handled inside MassiveClient).
    """
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)

    end = datetime.now(UTC).date()
    start = end - timedelta(days=5)

    for ticker in WATCHLIST_TICKERS:
        land_daily_bars(client, settings, ticker=ticker, start_date=start.isoformat(), end_date=end.isoformat())
        land_ticker_overview(client, settings, ticker=ticker)
        land_splits(client, settings, ticker=ticker)
        land_dividends(client, settings, ticker=ticker)
        land_news(client, settings, ticker=ticker)


if __name__ == "__main__":
    land_raw_json()
