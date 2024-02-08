from dagster import Definitions

from .assets import all_assets
from .resources import default_resources
from .schedules import all_jobs, all_schedules

# from .schedules import schedules

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources=default_resources,
)
