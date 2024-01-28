from dagster import AssetExecutionContext
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from ..resources import DBT_MANIFEST_PATH


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def f1warehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
