"""Runtime configuration shared by the ingestion job tasks.

Most values are read from environment variables so the same code runs
unchanged locally (via a `.env` file, see `.env.example`) and inside a
Databricks job, where the bundle injects them as task parameters / cluster
env vars. `MASSIVE_API_KEY` is the exception: serverless job tasks have no
`new_cluster` to attach `spark_env_vars`-style secret references to, so on
Databricks it's read via `dbutils.secrets.get()` instead - see
`_require_api_key()`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

SECRET_SCOPE = "ygz-massive-stock"


@dataclass(frozen=True)
class Settings:
    massive_api_key: str
    massive_base_url: str

    catalog: str
    landing_schema: str
    bronze_schema: str

    # Unity Catalog Volume path raw JSON responses are appended to,
    # e.g. /Volumes/<catalog>/<landing_schema>/raw.
    landing_volume_path: str

    @classmethod
    def from_env(cls) -> Settings:
        catalog = os.environ.get("DBT_CATALOG", "ygz_massive_stock_dev")
        landing_schema = os.environ.get("LANDING_SCHEMA", "landing")
        return cls(
            massive_api_key=_require_api_key(),
            massive_base_url=os.environ.get("MASSIVE_API_BASE_URL", "https://api.massive.com"),
            catalog=catalog,
            landing_schema=landing_schema,
            bronze_schema=os.environ.get("BRONZE_SCHEMA", "bronze"),
            # Not secret, and always this shape in practice (matches
            # databricks.yml's landing_volume_path variable default) - a
            # real job never sets it as an env var, so default it here
            # instead of requiring every job task to redeclare it.
            landing_volume_path=os.environ.get(
                "LANDING_VOLUME_PATH", f"/Volumes/{catalog}/{landing_schema}/raw_massive_data"
            ),
        )


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _require_api_key() -> str:
    """`MASSIVE_API_KEY` from the environment (local `.env`) if present,
    otherwise from the `ygz-massive-stock` Databricks secret scope - the
    real job runs on serverless compute, which has no cluster-level env
    var mechanism to inject secrets through, so the code has to fetch it
    directly. `databricks.sdk.runtime` only has a real `dbutils` when
    actually running in a Databricks job/notebook, so this stays a no-op
    fallback locally."""
    value = os.environ.get("MASSIVE_API_KEY")
    if value:
        return value
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(scope=SECRET_SCOPE, key="MASSIVE_API_KEY")
    except Exception as exc:
        raise RuntimeError("Missing required environment variable: MASSIVE_API_KEY") from exc
