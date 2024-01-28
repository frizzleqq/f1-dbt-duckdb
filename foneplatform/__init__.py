from dagster import Definitions

from .assets import all_assets
from .resources import dbt_resource, ergast_resource, io_manager

# from .schedules import schedules

defs = Definitions(
    assets=all_assets,
    # schedules=schedules,
    resources={
        "io_manager": io_manager.LocalCsvIOManager(),
        "dbt": dbt_resource,
        "ergast": ergast_resource.ErgastResource(),
    },
)
