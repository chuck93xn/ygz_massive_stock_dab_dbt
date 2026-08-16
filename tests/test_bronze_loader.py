from ingestion.bronze.loader import _exclude_existing_keys


class _FakeCatalog:
    def __init__(self, exists: bool):
        self._exists = exists

    def tableExists(self, table_name: str) -> bool:
        return self._exists


class _FakeSparkSession:
    """Stands in for SparkSession so the bootstrap (table-doesn't-exist-yet)
    branch of _exclude_existing_keys can be tested without a real Spark
    session. The "table exists, anti-join actually filters rows" branch
    needs real Spark join semantics and is verified against the real dev
    catalog instead - a local vanilla SparkSession doesn't work in this
    Windows .venv (no winutils), matching why this module has always been
    validated against real Databricks compute (see sketch/bronze_dev/)."""

    def __init__(self, *, table_exists: bool):
        self.catalog = _FakeCatalog(table_exists)


def test_exclude_existing_keys_first_run_returns_input_unchanged():
    spark = _FakeSparkSession(table_exists=False)
    sentinel = object()

    result = _exclude_existing_keys(spark, sentinel, "catalog.bronze.splits", ["split_id"])

    assert result is sentinel
