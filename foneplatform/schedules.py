from dagster import AssetSelection, ScheduleDefinition, define_asset_job

ergast_job = define_asset_job(
    name="ergast_job", selection=AssetSelection.groups("ergast").downstream()
)

ergast_schedule = ScheduleDefinition(
    job=ergast_job,
    cron_schedule="0 2 * * *",  # every day at 2am
)

all_jobs = [ergast_job]
all_schedules = [ergast_schedule]
