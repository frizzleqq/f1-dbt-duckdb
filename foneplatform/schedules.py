from dagster import AssetSelection, ScheduleDefinition, define_asset_job

ergast_job = define_asset_job(
    name="ergast_job", selection=AssetSelection.groups("ergast").downstream()
)

ergast_schedule = ScheduleDefinition(
    job=ergast_job,
    cron_schedule="0 2 * * *",  # every day at 2am
)

scheduled_jobs = [ergast_job]
schedules = [ergast_schedule]
