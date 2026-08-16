"""REST client for massive.com's market-data API.

API surface is Polygon.io-compatible (same endpoint shapes, field names,
and `next_url` pagination). Every endpoint/param below was verified against
the real API with a real key - see the matching demos under
`sketch/tutorial/` (DailyBars, TickerOverview, Splits, Dividends, News).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

DEFAULT_BASE_URL = "https://api.massive.com"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.5


class MassiveClientError(RuntimeError):
    pass


class MassiveClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        session: requests.Session | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = session or requests.Session()

    def get_daily_bars(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        *,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool = True,
    ) -> Iterator[dict[str, Any]]:
        """Yield raw daily-bar records (OHLCV) for `ticker` between the given dates."""
        path = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
        params = {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000}
        yield from self._paginate(path, params)

    def get_ticker_overview(self, ticker: str) -> dict[str, Any]:
        """Return the reference/profile record for a single ticker.

        Unlike the other methods, this endpoint isn't paginated - `results`
        is a single object, not a list, so this returns a dict directly
        instead of yielding records.
        """
        payload = self._request(f"{self.base_url}/v3/reference/tickers/{ticker}", {})
        return payload.get("results", {})

    def get_splits(self, ticker: str, *, limit: int = 250) -> Iterator[dict[str, Any]]:
        """Yield historical stock-split records for `ticker`."""
        params = {"ticker": ticker, "limit": limit, "sort": "execution_date.desc"}
        yield from self._paginate("/stocks/v1/splits", params)

    def get_dividends(self, ticker: str, *, limit: int = 250) -> Iterator[dict[str, Any]]:
        """Yield historical cash-dividend records for `ticker`."""
        params = {"ticker": ticker, "limit": limit, "sort": "ex_dividend_date.desc"}
        yield from self._paginate("/stocks/v1/dividends", params)

    def get_news(self, ticker: str, *, days_back: int = 7, limit: int = 5) -> Iterator[dict[str, Any]]:
        """Yield recent news articles mentioning `ticker`.

        `days_back`/`limit` default to the project's decision: last 7 days,
        max 5 articles per ticker (see plan/requirements/requirement_breakdown.md).

        `limit` is a hard cap on the total yielded, not just a per-page
        size - unlike get_splits/get_dividends (which intentionally return
        full history via unbounded pagination), news is meant to stay a
        short recent-headlines list, so this stops early instead of
        following next_url past `limit`.
        """
        since = (datetime.now(UTC) - timedelta(days=days_back)).strftime("%Y-%m-%d")
        params = {
            "ticker": ticker,
            "published_utc.gte": since,
            "limit": limit,
            "order": "desc",
            "sort": "published_utc",
        }
        for count, record in enumerate(self._paginate("/v2/reference/news", params)):
            if count >= limit:
                return
            yield record

    def _paginate(self, path: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        url = f"{self.base_url}{path}"
        query: dict[str, Any] | None = params
        while url:
            payload = self._request(url, query)
            yield from payload.get("results", [])

            url = payload.get("next_url")
            query = None  # next_url already carries its own query string

    def _request(self, url: str, params: dict[str, Any] | None) -> dict[str, Any]:
        request_params = dict(params or {})
        request_params["apiKey"] = self.api_key

        last_error: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=request_params, timeout=DEFAULT_TIMEOUT_SECONDS)
                if response.status_code == 429:
                    last_error = MassiveClientError(f"rate limited (429) on attempt {attempt + 1}")
                    self._sleep_for_retry(attempt, response)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                self._sleep_for_retry(attempt, None)

        raise MassiveClientError(
            f"Failed GET {url} after {MAX_RETRIES} attempts: {last_error}"
        ) from last_error

    @staticmethod
    def _sleep_for_retry(attempt: int, response: requests.Response | None) -> None:
        # massive.com's 429s don't send a standard Retry-After header - they
        # send x-ratelimit-reset (seconds until the rate-limit window
        # clears), which can be up to ~60s. The plain exponential backoff
        # below (a few seconds total) isn't long enough to wait that out.
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                time.sleep(float(retry_after))
                return
            reset_seconds = response.headers.get("x-ratelimit-reset")
            if reset_seconds is not None:
                time.sleep(float(reset_seconds) + 1)
                return
        time.sleep(BACKOFF_BASE_SECONDS**attempt)
