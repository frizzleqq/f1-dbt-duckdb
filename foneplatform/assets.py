from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .resources import DBT_MANIFEST_PATH


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def f1warehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
