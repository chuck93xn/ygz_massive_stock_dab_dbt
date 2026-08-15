"""Sanity check that MassiveClient's 5 real endpoints still work.

Usage (from .venv): python scripts/massive_api_check.py
Reads MASSIVE_API_KEY from `.env` at repo root.

If this hits massive.com's rate limit (429), it'll wait it out (up to
~60s, per the x-ratelimit-reset it returns) and retry automatically -
that's expected, not a hang.
"""

import os

from dotenv import load_dotenv

from ingestion.massive_client import MassiveClient


def main() -> None:
    load_dotenv()

    client = MassiveClient(api_key=os.environ["MASSIVE_API_KEY"])
    ticker = "AAPL"

    print("=== get_daily_bars ===")
    bars = list(client.get_daily_bars(ticker, "2026-08-05", "2026-08-15"))
    print(f"{len(bars)} bars, first: {bars[0] if bars else None}")

    print("\n=== get_ticker_overview ===")
    overview = client.get_ticker_overview(ticker)
    print(f"name={overview.get('name')}, market_cap={overview.get('market_cap')}")

    print("\n=== get_splits ===")
    splits = list(client.get_splits(ticker))
    print(f"{len(splits)} splits, most recent: {splits[0] if splits else None}")

    print("\n=== get_dividends ===")
    dividends = list(client.get_dividends(ticker))
    print(f"{len(dividends)} dividends, most recent: {dividends[0] if dividends else None}")

    print("\n=== get_news ===")
    news = list(client.get_news(ticker))
    print(f"{len(news)} articles (should be <= 5), first title: {news[0].get('title') if news else None}")


if __name__ == "__main__":
    main()
