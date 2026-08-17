"""DAB job entry point for promoting dev's Landing data into whichever
target this is deployed under (see pyproject.toml [project.scripts] +
databricks.yml targets.test/targets.prod resources.jobs.promote_from_dev_job
python_wheel_task - not resources/jobs.yml, since this job is only ever
deployed under targets.test/targets.prod, never the shared file - see
plan/records/08_bronze_promotion_process.md).

Source is hardcoded to dev's Landing Volume regardless of target - dev is
always the one real source of truth. Dest follows the deploying target via
Settings.from_job_argv() (the job passes ${var.catalog} as argv[1], same
mechanism land_daily_data/land_reference_data/load_bronze already use).
"""

from __future__ import annotations

from databricks.sdk.runtime import dbutils

from ingestion.promotion.copy_landing import copy_landing_volume
from ingestion.settings import Settings

DEV_LANDING_VOLUME_PATH = "/Volumes/ygz_massive_stock_dev/landing/raw_massive_data"


def copy_landing_from_dev() -> None:
    """Entry point for the `copy_landing_from_dev` DAB python_wheel_task."""
    settings = Settings.from_job_argv()
    copy_landing_volume(dbutils, source_path=DEV_LANDING_VOLUME_PATH, dest_path=settings.landing_volume_path)


if __name__ == "__main__":
    copy_landing_from_dev()
