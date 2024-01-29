from .dbt_resource import dbt_resource
from .ergast_resource import ErgastResource
from .io_manager import LocalCsvIOManager

default_resources = {
    "io_manager": LocalCsvIOManager(),
    "dbt": dbt_resource,
    "ergast": ErgastResource(),
}
