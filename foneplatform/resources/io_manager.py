import os
import textwrap
from typing import Optional

import pandas as pd
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    MemoizableIOManager,
    # TableSchemaMetadataValue,
)
from dagster._core.definitions.metadata import MetadataValue
from pydantic import Field


class LocalCsvIOManager(ConfigurableIOManager, MemoizableIOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    base_path: Optional[str] = Field(default=os.path.join("data"))
    extension: Optional[str] = Field(default="csv")

    def _get_fs_path(self, asset_key: AssetKey) -> str:
        rpath = f"{os.path.join(self.base_path, *asset_key.path)}.{self.extension}"
        return os.path.abspath(rpath)

    def handle_output(self, context, df: pd.DataFrame):
        """This saves the dataframe as a CSV."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        df.to_csv(fpath, encoding="utf8", index=False)
        with open(fpath + ".version", "w", encoding="utf8") as f:
            f.write(context.version if context.version else "None")

        context.add_output_metadata(
            {
                "Rows": MetadataValue.int(df.shape[0]),
                "Path": MetadataValue.path(fpath),
                "Sample": MetadataValue.md(df.head(5).to_markdown()),
                "Resolved version": MetadataValue.text(context.version),
                # "Schema": MetadataValue.table_schema(
                #     self.get_schema(context.dagster_type)
                # ),
            }
        )

    def get_schema(self, dagster_type):
        pass

    def load_input(self, context):
        """This reads a dataframe from a CSV."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        date_col_names = [
            table_col.name
            for table_col in self.get_schema(
                context.upstream_output.dagster_type
            ).columns
            if table_col.type == "datetime64[ns]"
        ]
        return pd.read_csv(fpath, parse_dates=date_col_names)

    def has_output(self, context) -> bool:
        fpath = self._get_fs_path(asset_key=context.asset_key)
        version_fpath = fpath + ".version"
        if not os.path.exists(version_fpath):
            return False
        with open(version_fpath, "r", encoding="utf8") as f:
            version = f.read()

        return version == context.version


def pandas_columns_to_markdown(dataframe: pd.DataFrame) -> str:
    return (
        textwrap.dedent(
            """
        | Name | Type |
        | ---- | ---- |
    """
        )
        + "\n".join(
            [f"| {name} | {dtype} |" for name, dtype in dataframe.dtypes.items()]
        )
    )
