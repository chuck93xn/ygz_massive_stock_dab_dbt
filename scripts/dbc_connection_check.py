"""Quick sanity check that venv_dbc can open a serverless Spark session.

Usage (from .venv_dbc): python scripts/dbc_connection_check.py
Reads DATABRICKS_CONFIG_PROFILE (and/or DATABRICKS_HOST + DATABRICKS_TOKEN)
from `.env` at repo root; falls back to `databricks auth login` state if
`.env` doesn't set a profile.
"""

from databricks.connect import DatabricksSession
from dotenv import load_dotenv


def main() -> None:
    load_dotenv()
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    spark.sql("select 1 as ok").show()


if __name__ == "__main__":
    main()
