import os

from dagster_dbt import DbtCliResource

from .dbt_resource import dbt_project
from .io_manager import LocalParquetIOManager

# ensure DATA_DIR is absolute path so both dagster and dbt can use it.
# dbt (run through dagster) has a different working directory than dagster.
if "DATA_DIR" not in os.environ:
    raise ValueError("DATA_DIR environment variable is not set")

os.environ["DATA_DIR"] = os.path.abspath(os.environ["DATA_DIR"])

default_resources = {
    "io_manager": LocalParquetIOManager(base_path=os.environ["DATA_DIR"]),
    "dbt": DbtCliResource(project_dir=dbt_project),
}
