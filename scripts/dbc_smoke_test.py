"""Quick sanity check that venv_dbc can open a serverless Spark session.

Usage (from venv_dbc): python scripts/dbc_smoke_test.py
Requires DATABRICKS_HOST + DATABRICKS_TOKEN in the environment, or a
configured `databricks auth login` profile.
"""

from databricks.connect import DatabricksSession


def main() -> None:
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    spark.sql("select 1 as ok").show()


if __name__ == "__main__":
    main()
