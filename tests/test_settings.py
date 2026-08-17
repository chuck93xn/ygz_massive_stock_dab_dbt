from ingestion.settings import Settings


def test_from_env_catalog_override_takes_precedence_over_env_var(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setenv("DBT_CATALOG", "ygz_massive_stock_dev")

    settings = Settings.from_env(catalog_override="ygz_massive_stock_prod")

    assert settings.catalog == "ygz_massive_stock_prod"
    assert settings.landing_volume_path == "/Volumes/ygz_massive_stock_prod/landing/raw_massive_data"


def test_from_env_falls_back_to_env_var_then_default(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.delenv("DBT_CATALOG", raising=False)

    assert Settings.from_env().catalog == "ygz_massive_stock_dev"

    monkeypatch.setenv("DBT_CATALOG", "some_other_catalog")
    assert Settings.from_env().catalog == "some_other_catalog"


def test_from_job_argv_uses_argv_when_present(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr("sys.argv", ["load_bronze", "ygz_massive_stock_prod"])

    assert Settings.from_job_argv().catalog == "ygz_massive_stock_prod"


def test_from_job_argv_falls_back_to_from_env_without_argv(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.delenv("DBT_CATALOG", raising=False)
    monkeypatch.setattr("sys.argv", ["load_bronze"])

    assert Settings.from_job_argv().catalog == "ygz_massive_stock_dev"
