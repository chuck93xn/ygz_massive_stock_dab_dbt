"""Landing layer: append raw massive.com JSON to a Unity Catalog Volume.

Files are partitioned by ingestion date and never modified after being
written, so the Landing layer stays a durable, replayable record of exactly
what massive.com returned (request metadata included, for traceability).

Two ways to actually write the bytes, since `/Volumes/...` paths are only a
real local filesystem path when FUSE-mounted inside a Databricks
cluster/job:
- `write_landing_records` - plain `Path.open()`. Correct (and required) when
  running as the `land_raw_json` DAB job task.
- `write_landing_records_via_files_api` - Databricks Files API upload.
  Correct for local dev/backfills, where `/Volumes/...` isn't mounted.

Both share `_build_ndjson` for the actual serialization so the two paths
can't drift.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ingestion.massive_client import MassiveClient
from ingestion.settings import Settings

Writer = Callable[..., str]

# Fixed watchlist (see plan/requirement_breakdown.md) - not small-cap
# specific, just the 10 tickers the project tracks.
WATCHLIST_TICKERS = ["AAPL", "MSFT", "AMZN", "JPM", "JNJ", "TSLA", "XOM", "KO", "DIS", "BA"]


def _build_ndjson(records: Iterable[dict[str, Any]], request_metadata: dict[str, Any]) -> str:
    ingested_at = datetime.now(UTC).isoformat()
    lines = [
        json.dumps({"record": record, "_ingested_at": ingested_at, "_request_metadata": request_metadata})
        for record in records
    ]
    return "\n".join(lines) + "\n"


def _partition_path(landing_volume_path: str, source_name: str, run_date: date | None) -> str:
    run_date = run_date or datetime.now(UTC).date()
    return f"{landing_volume_path.rstrip('/')}/{source_name}/date={run_date.isoformat()}/{uuid.uuid4()}.jsonl"


def write_landing_records(
    records: Iterable[dict[str, Any]],
    *,
    landing_volume_path: str,
    source_name: str,
    request_metadata: dict[str, Any],
    run_date: date | None = None,
) -> str:
    """Write via the local filesystem. Only works where `/Volumes` is
    FUSE-mounted, i.e. running inside a Databricks cluster/job."""
    content = _build_ndjson(records, request_metadata)
    file_path = _partition_path(landing_volume_path, source_name, run_date)

    path_obj = Path(file_path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    path_obj.write_text(content, encoding="utf-8")

    return file_path


def write_landing_records_via_files_api(
    records: Iterable[dict[str, Any]],
    *,
    workspace_client: Any,
    landing_volume_path: str,
    source_name: str,
    request_metadata: dict[str, Any],
    run_date: date | None = None,
) -> str:
    """Write via the Databricks Files API - for local dev/backfills.

    `workspace_client` is a `databricks.sdk.WorkspaceClient`, kept as a
    generic-typed param here so this module doesn't need `databricks-sdk`
    as a hard import-time dependency (it's already in pyproject.toml, but
    the Bronze/job path never needs it).
    """
    import io

    content = _build_ndjson(records, request_metadata)
    file_path = _partition_path(landing_volume_path, source_name, run_date)

    workspace_client.files.upload(file_path, io.BytesIO(content.encode("utf-8")), overwrite=True)

    return file_path


def land_daily_bars(
    client: MassiveClient,
    settings: Settings,
    *,
    ticker: str,
    start_date: str,
    end_date: str,
    writer: Writer = write_landing_records,
) -> str:
    bars = list(client.get_daily_bars(ticker, start_date, end_date))
    return writer(
        bars,
        landing_volume_path=settings.landing_volume_path,
        source_name="daily_bars",
        request_metadata={
            "endpoint": f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}",
            "ticker": ticker,
        },
    )


def main() -> None:
    """Entry point for the `land_raw_json` DAB python_wheel_task.

    Daily incremental pull: a short recent window per watchlist ticker, so a
    normal run just tops up the last few days (Silver dedups any overlap).
    For the one-time full-history load, see scripts/backfill_daily_bars.py.
    """
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)

    end = datetime.now(UTC).date()
    start = end - timedelta(days=5)

    for ticker in WATCHLIST_TICKERS:
        land_daily_bars(client, settings, ticker=ticker, start_date=start.isoformat(), end_date=end.isoformat())


if __name__ == "__main__":
    main()
