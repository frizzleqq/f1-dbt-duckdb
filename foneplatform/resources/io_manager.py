import logging
import os
import textwrap

import dagster
import pandas as pd
from dagster import AssetKey, ConfigurableIOManager
from dagster._core.definitions.metadata import MetadataValue
from pydantic import Field


class LocalCsvIOManager(ConfigurableIOManager):  # type: ignore
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    base_path: str = Field(default="data")
    extension: str = Field(default="csv")

    @property
    def _log(self) -> logging.Logger:
        return dagster.get_dagster_logger()

    def _get_fs_path(self, asset_key: AssetKey) -> str:
        rpath = f"{os.path.join(self.base_path, *asset_key.path)}.{self.extension}"
        return os.path.abspath(rpath)

    def handle_output(self, context, df: pd.DataFrame):
        """This saves the dataframe as a CSV."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        self._log.info(f"Saving dataframe to '{fpath}'")
        df.to_csv(fpath, encoding="utf8", index=False)

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

    # def get_schema(self, dagster_type):
    #     pass

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


def pandas_columns_to_markdown(dataframe: pd.DataFrame) -> str:
    return textwrap.dedent(
        """
        | Name | Type |
        | ---- | ---- |
    """
    ) + "\n".join([f"| {name} | {dtype} |" for name, dtype in dataframe.dtypes.items()])
