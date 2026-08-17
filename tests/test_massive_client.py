from ingestion.landing.client import MassiveClient, _rate_limit_headers


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.headers: dict = {}

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, pages: dict[str, dict]):
        self._pages = pages
        self.calls: list[str] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(url)
        return _FakeResponse(self._pages[url])


def test_get_dividends_follows_next_url():
    base = "https://api.massive.com/stocks/v1/dividends"
    next_page = f"{base}?cursor=abc"
    pages = {
        base: {"results": [{"cash_amount": 0.26}], "next_url": next_page},
        next_page: {"results": [{"cash_amount": 0.25}]},
    }
    session = _FakeSession(pages)
    # request_delay_seconds=0: the real client paces successful calls a
    # second apart (see DEFAULT_REQUEST_DELAY_SECONDS) to avoid the
    # tight-loop 429s hit in real testing - not needed against a fake
    # session, and would just slow this test down for no reason.
    client = MassiveClient(api_key="test-key", session=session, request_delay_seconds=0)

    records = list(client.get_dividends("AAPL"))

    assert records == [{"cash_amount": 0.26}, {"cash_amount": 0.25}]
    assert session.calls == [base, next_page]


def test_get_ticker_overview_returns_single_dict_not_paginated():
    url = "https://api.massive.com/v3/reference/tickers/AAPL"
    session = _FakeSession({url: {"results": {"ticker": "AAPL", "name": "Apple Inc."}}})
    client = MassiveClient(api_key="test-key", session=session, request_delay_seconds=0)

    result = client.get_ticker_overview("AAPL")

    assert result == {"ticker": "AAPL", "name": "Apple Inc."}
    assert session.calls == [url]


def test_rate_limit_headers_picks_out_limit_and_retry_headers():
    response = _FakeResponse({}, status_code=429)
    response.headers = {
        "x-ratelimit-reset": "30",
        "x-ratelimit-limit": "5",
        "Retry-After": "30",
        "Content-Type": "application/json",
    }

    assert _rate_limit_headers(response) == {
        "x-ratelimit-reset": "30",
        "x-ratelimit-limit": "5",
        "Retry-After": "30",
    }


def test_rate_limit_headers_empty_when_vendor_sends_nothing_useful():
    response = _FakeResponse({}, status_code=429)
    response.headers = {"Content-Type": "application/json"}

    assert _rate_limit_headers(response) == {}
