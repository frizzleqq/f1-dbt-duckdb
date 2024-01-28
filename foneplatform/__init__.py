from dagster import Definitions

from .assets import all_assets
from . import resources
# from .schedules import schedules

defs = Definitions(
    assets=all_assets,
    # schedules=schedules,
    resources={
        "io_manager": resources.LocalCsvIOManager(),
        "dbt": resources.dbt_resource,
        "ergast": resources.ErgastResource(),
    },
)
