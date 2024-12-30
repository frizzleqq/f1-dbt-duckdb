import os
import sys
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

dbt_project = DbtProject(project_dir=Path(__file__).parent.parent.parent.joinpath("dbt").resolve())


def get_dbt_executable() -> str:
    """
    Returns the path to the dbt executable.

    If we're in a virtualenv, we want to use the dbt executable in the virtualenv.
    """
    if sys.prefix != sys.base_prefix:
        if os.name == "nt":
            return os.path.join(sys.prefix, "Scripts", "dbt")
        else:
            return os.path.join(sys.prefix, "bin", "dbt")
    else:
        return "dbt"


def get_dbt_resource() -> DbtCliResource:
    """
    Returns a DbtCliResource.

    On 'dagster dev', a manifest will be created at run time.
    """
    dbt_resource = DbtCliResource(
        project_dir=dbt_project,
        profiles_dir=os.fspath(dbt_project.project_dir),
        dbt_executable=get_dbt_executable(),
    )
    dbt_project.prepare_if_dev()
    return dbt_resource
