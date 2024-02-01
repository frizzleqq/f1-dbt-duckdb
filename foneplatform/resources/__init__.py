from dagster import EnvVar

from .dbt_resource import get_dbt_resource
from .ergast_resource import ErgastResource
from .io_manager import LocalCsvIOManager

default_resources = {
    "io_manager": LocalCsvIOManager(base_path=EnvVar("DATA_DIR")),
    "dbt": get_dbt_resource(),
    "ergast": ErgastResource(),
}
