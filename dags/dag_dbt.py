import os
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = os.environ["DBT_DIR"]

default_args = {"start_date": datetime.datetime(2022, 1, 1)}


def dbt_operator(task_id: str, command: str):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR}; {command}",
    )


with DAG(
    dag_id="dbt_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    dbt_run = dbt_operator("dbt_run", "dbt run")
    dbt_test = dbt_operator("dbt_test", "dbt test")

    dbt_run >> dbt_test
