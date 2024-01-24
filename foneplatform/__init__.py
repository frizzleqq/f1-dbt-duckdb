from dagster import Definitions

from .assets import f1warehouse_dbt_assets
from .resources import dbt_resource
from .schedules import schedules

defs = Definitions(
    assets=[f1warehouse_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
    },
)
