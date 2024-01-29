from dagster import AssetExecutionContext, Config
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from ..resources.dbt_resource import DBT_MANIFEST_PATH


class DbtConfig(Config): # type: ignore
    full_refresh: bool = False


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def f1warehouse_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig
):
    args = ["build", "--full-refresh"] if config.full_refresh else ["build"]
    yield from dbt.cli(args, context=context).stream()
