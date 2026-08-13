"""REST client for massive.com's market-data API.

API surface is Polygon.io-compatible (same endpoint shapes, field names,
and `next_url` pagination) - confirmed against
https://massive.com/docs/rest/stocks/aggregates/custom-bars.md and
https://massive.com/docs/rest/stocks/tickers/all-tickers.md. Auth is assumed
to be the same `apiKey` query-param convention Polygon uses; that part
wasn't in the fetched docs, so verify it empirically with a real key before
trusting it in production (a 401 means try `Authorization: Bearer` instead).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
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
        """Yield raw daily-bar records (OHLCV) for `ticker` between the given dates.

        Each record has Polygon/Massive's short field names: o/h/l/c/v/vw/n/t.
        """
        path = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
        params = {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000}
        yield from self._paginate(path, params)

    def get_tickers(self, *, market: str = "stocks", active: bool = True) -> Iterator[dict[str, Any]]:
        """Yield raw ticker/reference-data records."""
        params = {"market": market, "active": str(active).lower(), "limit": 1000}
        yield from self._paginate("/v3/reference/tickers", params)

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
                    self._sleep_for_retry(attempt, response.headers.get("Retry-After"))
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                self._sleep_for_retry(attempt, None)

        raise MassiveClientError(f"Failed GET {url} after {MAX_RETRIES} attempts") from last_error

    @staticmethod
    def _sleep_for_retry(attempt: int, retry_after: str | None) -> None:
        if retry_after is not None:
            time.sleep(float(retry_after))
        else:
            time.sleep(BACKOFF_BASE_SECONDS**attempt)
