import os
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
DBT_PROJECT_DIR = os.environ["DBT_PROJECT_DIR"]

default_args = {"owner": "airflow", "start_date": datetime.datetime(2022, 1, 1)}


def dbt_operator(
    task_id: str, dbt_command: str, dbt_args: str = "", dbt_project_dir: str = None
):
    if dbt_project_dir is None:
        dbt_project_dir = DBT_PROJECT_DIR
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"dbt {dbt_command} {dbt_args}"
            f" --project-dir='{dbt_project_dir}' --profiles-dir='{dbt_project_dir}'"
        ),
    )


with DAG(
    dag_id="dbt_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_dbt:
    dbt_deps = dbt_operator("dbt_deps", "deps")
    dbt_run_stage = dbt_operator(
        "dbt_run_stage", "run", dbt_args="--select 'staging'"
    )
    dbt_test_stage = dbt_operator(
        "dbt_test_stage", "test", dbt_args="--select 'staging'"
    )
    dbt_run_core = dbt_operator("dbt_run_core", "run", dbt_args="--select 'core'")
    dbt_run_snapshots = dbt_operator("dbt_run_snapshots", "snapshot")
    dbt_test_core = dbt_operator(
        "dbt_test_core", "test", dbt_args="--select 'core'"
    )

    dbt_deps >> dbt_run_stage >> dbt_test_stage
    dbt_test_stage >> dbt_run_core >> dbt_test_core
    dbt_test_core >> dbt_run_snapshots


with DAG(
    dag_id="dbt_docs",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_dbt_docs:
    dbt_docs = dbt_operator("dbt_docs", "docs generate")
