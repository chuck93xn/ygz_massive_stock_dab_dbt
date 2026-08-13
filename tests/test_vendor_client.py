from ingestion.vendor_client import VendorClient


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
    def __init__(self, pages: list[dict]):
        self._pages = pages
        self.calls = 0

    def get(self, url, params=None, headers=None, timeout=None):
        page = self._pages[self.calls]
        self.calls += 1
        return _FakeResponse(page)


def test_paginate_follows_next_cursor():
    pages = [
        {"results": [{"ticker": "AAA"}], "next_cursor": "page-2"},
        {"results": [{"ticker": "BBB"}], "next_cursor": None},
    ]
    session = _FakeSession(pages)
    client = VendorClient(base_url="https://example.invalid", api_key="test-key", session=session)

    records = list(client.get_tickers())

    assert records == [{"ticker": "AAA"}, {"ticker": "BBB"}]
    assert session.calls == 2
