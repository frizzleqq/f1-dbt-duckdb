from dagster import Definitions

from .assets import foneplatform_dbt_assets
from .resources import dbt_resource, ergast_resource
from .schedules import schedules

defs = Definitions(
    assets=[foneplatform_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
        "ergast": ergast_resource,
    },
)
