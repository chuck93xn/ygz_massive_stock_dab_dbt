"""Runtime configuration shared by the ingestion job tasks.

Values are read from environment variables so the same code runs unchanged
locally (via a `.env` file, see `.env.example`) and inside a Databricks job,
where the job injects them as task parameters / cluster env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # TODO: rename once the actual market-data vendor is chosen.
    vendor_base_url: str
    vendor_api_key: str

    catalog: str
    landing_schema: str
    bronze_schema: str

    # Unity Catalog Volume path raw JSON responses are appended to,
    # e.g. /Volumes/<catalog>/<landing_schema>/raw.
    landing_volume_path: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            vendor_base_url=_require("VENDOR_API_BASE_URL"),
            vendor_api_key=_require("VENDOR_API_KEY"),
            catalog=os.environ.get("DBT_CATALOG", "ygz_massive_stock_dev"),
            landing_schema=os.environ.get("LANDING_SCHEMA", "landing"),
            bronze_schema=os.environ.get("BRONZE_SCHEMA", "bronze"),
            landing_volume_path=_require("LANDING_VOLUME_PATH"),
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
