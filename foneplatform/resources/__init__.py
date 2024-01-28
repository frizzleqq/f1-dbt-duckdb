import os
from pathlib import Path

from dagster_dbt import DbtCliResource

from .ergast_resource import ErgastResource
from .io_manager import LocalCsvIOManager

dbt_project_dir = Path(__file__).parent.parent.parent.joinpath("dbt").resolve()
DBT_MANIFEST_PATH = dbt_project_dir.joinpath("target", "manifest.json")


dbt_resource = DbtCliResource(
    project_dir=os.fspath(dbt_project_dir),
    profiles_dir=os.fspath(dbt_project_dir),
)

# If DAGSTER_DBT_PARSE_PROJECT_ON_LOAD is set, a manifest will be created at run time.
# Otherwise, we expect a manifest to be present in the project's target directory.
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    (
        dbt_resource.cli(
            ["--quiet", "parse"],
            target_path=Path("target"),
        )
        .wait()
        .target_path.joinpath("manifest.json")
    )
