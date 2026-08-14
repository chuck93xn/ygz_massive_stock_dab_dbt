"""Thin REST client for the (TBD) market-data vendor.

This is a placeholder: auth scheme, endpoint paths, param names, and
pagination shape all depend on which vendor ends up being used. Wire the
real API in once that's decided; the retry/backoff and pagination loop
below are generic enough to keep either way.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import requests

DEFAULT_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 1.5


class VendorClientError(RuntimeError):
    pass


class VendorClient:
    def __init__(self, base_url: str, api_key: str, session: requests.Session | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = session or requests.Session()

    def get_daily_bars(self, ticker: str, start_date: str, end_date: str) -> Iterator[dict[str, Any]]:
        """Yield raw daily-bar records for `ticker` between the given dates.

        TODO: replace the placeholder path/params below with the real
        vendor endpoint once it's known.
        """
        params = {
            "ticker": ticker,
            "start": start_date,
            "end": end_date,
        }
        yield from self._paginate("/v1/daily-bars", params)

    def get_tickers(self) -> Iterator[dict[str, Any]]:
        """Yield raw ticker/reference-data records.

        TODO: replace the placeholder path once the real endpoint is known.
        """
        yield from self._paginate("/v1/tickers", {})

    def _paginate(self, path: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor

            payload = self._request(path, page_params)
            yield from payload.get("results", [])

            cursor = payload.get("next_cursor")
            if not cursor:
                return

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}

        last_error: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(
                    url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT_SECONDS
                )
                if response.status_code == 429:
                    self._sleep_for_retry(attempt, response.headers.get("Retry-After"))
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                self._sleep_for_retry(attempt, None)

        raise VendorClientError(f"Failed GET {url} after {MAX_RETRIES} attempts") from last_error

    @staticmethod
    def _sleep_for_retry(attempt: int, retry_after: str | None) -> None:
        if retry_after is not None:
            time.sleep(float(retry_after))
        else:
            time.sleep(BACKOFF_BASE_SECONDS**attempt)
