from dagster import AssetExecutionContext, Config
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    dbt_assets,
)

from ..resources.dbt_resource import dbt_project


class DbtConfig(Config):  # type: ignore
    full_refresh: bool = False


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def f1warehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    args = ["build", "--full-refresh"] if config.full_refresh else ["build"]
    # fetch-metadata does not work with motherduck
    # yield from dbt.cli(args, context=context).stream().fetch_column_metadata().fetch_row_counts()
    yield from dbt.cli(args, context=context).stream()
