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
        bash_command=f"cd {dbt_project_dir}; {dbt_command} {dbt_args}",
    )


with DAG(
    dag_id="dbt_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_dbt:
    dbt_deps = dbt_operator("dbt_deps", "dbt deps")
    dbt_run_stage = dbt_operator(
        "dbt_run_stage", "dbt run", dbt_args="--select 'staging'"
    )
    dbt_test_stage = dbt_operator(
        "dbt_test_stage", "dbt test", dbt_args="--select 'staging'"
    )
    dbt_run_core = dbt_operator("dbt_run_core", "dbt run", dbt_args="--select 'core'")
    dbt_test_core = dbt_operator(
        "dbt_test_core", "dbt test", dbt_args="--select 'core'"
    )

    dbt_deps >> dbt_run_stage >> dbt_test_stage
    dbt_test_stage >> dbt_run_core >> dbt_test_core


with DAG(
    dag_id="dbt_docs",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_dbt_docs:
    dbt_docs = dbt_operator("dbt_docs", "dbt docs generate")
