"""DAB job entry points for the Landing layer (see pyproject.toml
[project.scripts] + resources/jobs.yml python_wheel_task). These are the
only functions under ingestion/landing/ with real entry points - client.py
and writer.py are pure functions.

Split into two entry points by how often each source actually needs
refreshing, instead of one job landing all 5 sources together:
- land_daily_data(): daily_bars + news - the two sources that genuinely
  change day to day, meant to run on a daily schedule.
- land_reference_data(): ticker_overview + splits + dividends - a
  company's profile, split history, and dividend history don't move fast
  enough to need a daily pull, meant to run weekly instead.

A single combined run was 10 tickers x 5 sources = 50 massive.com calls
back-to-back, which is what triggered real 429s in testing (see
plan/records/05_job_serverless_process.md). Splitting cuts the job that
actually needs to run daily down to 20 calls, and moves the other 30 to a
job that only needs to run once a week.
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


def land_daily_data() -> None:
    """Entry point for the `land_daily_data` DAB python_wheel_task - meant
    to run daily. daily_bars: a short recent window (Silver dedups any
    overlap; for the one-time full-history load, see
    scripts/landing/backfill_daily_bars.py). news: MassiveClient.get_news's
    own default window (last 7 days, top 5 per ticker - see
    plan/requirements/requirement_breakdown.md).

    Any single API call raising propagates and fails the whole job -
    fail-fast, no retry/skip logic here (retries are already handled
    inside MassiveClient).
    """
    settings = Settings.from_job_argv()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)

    end = datetime.now(UTC).date()
    start = end - timedelta(days=5)

    for ticker in WATCHLIST_TICKERS:
        land_daily_bars(client, settings, ticker=ticker, start_date=start.isoformat(), end_date=end.isoformat())
        land_news(client, settings, ticker=ticker)


def land_reference_data() -> None:
    """Entry point for the `land_reference_data` DAB python_wheel_task -
    meant to run weekly, not daily. ticker_overview: today's snapshot -
    still what feeds Gold's dim_ticker_snapshot SCD2 detection, it just
    doesn't need checking every single day for 10 mega-cap tickers whose
    profile essentially never changes. splits/dividends: the vendor
    endpoints return full history on every call regardless of how often
    you ask, so pulling them weekly instead of daily doesn't lose
    anything - a split/dividend landing up to a week after it happened is
    fine at this project's watchlist scale.
    """
    settings = Settings.from_job_argv()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)

    for ticker in WATCHLIST_TICKERS:
        land_ticker_overview(client, settings, ticker=ticker)
        land_splits(client, settings, ticker=ticker)
        land_dividends(client, settings, ticker=ticker)


if __name__ == "__main__":
    land_daily_data()
