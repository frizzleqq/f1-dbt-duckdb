import os
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

STAGING_DIR = os.environ["STAGING_DIR"]

default_args = {"owner": "airflow", "start_date": datetime.datetime(2022, 1, 1)}


def staging_operator(task_id: str, tables: list = None, read_full: bool = False):
    if tables is None:
        tables = []
    read_arg = "--read-full" if read_full else ""
    return BashOperator(
        task_id=task_id,
        bash_command=f"python {STAGING_DIR}/staging.py {' '.join(tables)} {read_arg}",
    )


with DAG(
    dag_id="staging",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_staging:
    stage_tables = staging_operator("stage_tables")


with DAG(
    dag_id="staging-full_load",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag_staging_full:
    stage_tables_full_load = staging_operator("stage_tables_full_load", read_full=True)
