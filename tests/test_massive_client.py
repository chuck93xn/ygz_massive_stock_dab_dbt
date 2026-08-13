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


def test_paginate_follows_next_url():
    base = "https://api.massive.com/v3/reference/tickers"
    next_page = f"{base}?cursor=abc"
    pages = {
        base: {"results": [{"ticker": "AAA"}], "next_url": next_page},
        next_page: {"results": [{"ticker": "BBB"}]},
    }
    session = _FakeSession(pages)
    client = MassiveClient(api_key="test-key", session=session)

    records = list(client.get_tickers())

    assert records == [{"ticker": "AAA"}, {"ticker": "BBB"}]
    assert session.calls == [base, next_page]
