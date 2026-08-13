"""Landing layer: append raw vendor JSON to a Unity Catalog Volume.

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

from ingestion.settings import Settings
from ingestion.vendor_client import VendorClient


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


def main() -> None:
    """Entry point for the `land_raw_json` DAB python_wheel_task.

    TODO: once the vendor + universe of tickers is decided, replace this
    with the real fetch loop (which tickers, which date range per run).
    """
    settings = Settings.from_env()
    client = VendorClient(base_url=settings.vendor_base_url, api_key=settings.vendor_api_key)

    tickers = list(client.get_tickers())
    write_landing_records(
        tickers,
        landing_volume_path=settings.landing_volume_path,
        source_name="tickers",
        request_metadata={"endpoint": "/v1/tickers"},
    )


if __name__ == "__main__":
    main()
