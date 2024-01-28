from . import dbt, ergast

from dagster import load_assets_from_modules

dbt_assets = load_assets_from_modules([dbt], group_name="dbt")
ergast_assets = load_assets_from_modules(
    [ergast], group_name="ergast", key_prefix="ergast"
)

all_assets = [*dbt_assets, *ergast_assets]
