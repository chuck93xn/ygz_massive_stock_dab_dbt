from ingestion.massive_client import MassiveClient


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
    client = MassiveClient(api_key="test-key", session=session)

    records = list(client.get_dividends("AAPL"))

    assert records == [{"cash_amount": 0.26}, {"cash_amount": 0.25}]
    assert session.calls == [base, next_page]


def test_get_ticker_overview_returns_single_dict_not_paginated():
    url = "https://api.massive.com/v3/reference/tickers/AAPL"
    session = _FakeSession({url: {"results": {"ticker": "AAPL", "name": "Apple Inc."}}})
    client = MassiveClient(api_key="test-key", session=session)

    result = client.get_ticker_overview("AAPL")

    assert result == {"ticker": "AAPL", "name": "Apple Inc."}
    assert session.calls == [url]
