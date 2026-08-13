"""Landing layer: append raw massive.com JSON to a Unity Catalog Volume.

Files are partitioned by ingestion date and never modified after being
written, so the Landing layer stays a durable, replayable record of exactly
what the vendor returned (request metadata included, for traceability).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ingestion.massive_client import MassiveClient
from ingestion.settings import Settings


def write_landing_records(
    records: Iterable[dict[str, Any]],
    *,
    landing_volume_path: str,
    source_name: str,
    request_metadata: dict[str, Any],
    run_date: date | None = None,
) -> str:
    """Write `records` as newline-delimited JSON under a date partition.

    Returns the path written to, so callers (e.g. the Bronze loader) can
    record it as `_source_file` for audit purposes.
    """
    run_date = run_date or datetime.now(UTC).date()
    partition_dir = Path(landing_volume_path) / source_name / f"date={run_date.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_path = partition_dir / f"{uuid.uuid4()}.jsonl"
    ingested_at = datetime.now(UTC).isoformat()

    with file_path.open("w", encoding="utf-8") as f:
        for record in records:
            envelope = {
                "record": record,
                "_ingested_at": ingested_at,
                "_request_metadata": request_metadata,
            }
            f.write(json.dumps(envelope) + "\n")

    return str(file_path)


def land_tickers(client: MassiveClient, settings: Settings) -> str:
    tickers = list(client.get_tickers())
    return write_landing_records(
        tickers,
        landing_volume_path=settings.landing_volume_path,
        source_name="tickers",
        request_metadata={"endpoint": "/v3/reference/tickers"},
    )


def land_daily_bars(
    client: MassiveClient, settings: Settings, *, ticker: str, start_date: str, end_date: str
) -> str:
    bars = list(client.get_daily_bars(ticker, start_date, end_date))
    return write_landing_records(
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

    Lands the full tickers reference list. Daily-bar landing
    (`land_daily_bars`) isn't wired in here yet - which tickers and date
    range to pull per run is still an open call.
    """
    settings = Settings.from_env()
    client = MassiveClient(api_key=settings.massive_api_key, base_url=settings.massive_base_url)
    land_tickers(client, settings)


if __name__ == "__main__":
    main()
