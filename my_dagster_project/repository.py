from dagster import (
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    repository,
)

from my_dagster_project import assets

daily_job = define_asset_job(name="daily_refresh", selection="*")
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="@daily",
)


@repository
def my_dagster_project():
    return [daily_job, daily_schedule, load_assets_from_package_module(assets)]
