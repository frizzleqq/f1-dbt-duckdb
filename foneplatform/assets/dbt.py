from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
)

from ..resources import DBT_MANIFEST_PATH


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def foneplatform_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
