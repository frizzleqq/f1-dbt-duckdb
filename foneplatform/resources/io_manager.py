import logging
import os

import dagster
import duckdb
from dagster import AssetKey, ConfigurableIOManager
from dagster._core.definitions.metadata import MetadataValue
from pydantic import Field


def _get_dagster_table_schema(df: duckdb.DuckDBPyRelation):
    """Returns a TableSchema for the given duckdb dataframe."""

    return dagster.TableSchema(
        columns=[dagster.TableColumn(name=col[0], type=col[1]) for col in df.description]
    )


class LocalCsvIOManager(ConfigurableIOManager):  # type: ignore
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    base_path: str = Field(description="The base path for the CSVs.")
    extension: str = Field(description="The file extension for the CSVs.", default="csv")

    @property
    def _log(self) -> logging.Logger:
        return dagster.get_dagster_logger()

    def _get_fs_path(self, asset_key: AssetKey) -> str:
        rpath = f"{os.path.join(self.base_path, *asset_key.path)}.{self.extension}"
        return os.path.abspath(rpath)

    def handle_output(self, context, df: duckdb.DuckDBPyRelation):
        """This saves the dataframe as a CSV."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        self._log.info(f"Saving dataframe to '{fpath}'")
        df.to_csv(fpath, header=True, sep=",", encoding="utf-8", quotechar='"')

        context.add_output_metadata(
            {
                "dagster/row_count": MetadataValue.int(df.count("*").fetchall()[0]),
                "path": MetadataValue.path(fpath),
                # "sample": MetadataValue.md(df.head(5).to_markdown()),
                "dagster/column_schema": _get_dagster_table_schema(df),
            }
        )

    def load_input(self, context) -> duckdb.DuckDBPyRelation:
        """This reads a dataframe from a CSV."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        return duckdb.read_csv(
            fpath,
            header=True,
            delimiter=",",
            encoding="utf-8",
            quotechar='"',
            connection=duckdb.connect(":default:"),
        )


class LocalParquetIOManager(ConfigurableIOManager):  # type: ignore
    """Translates between Pandas DataFrames and parquet on the local filesystem."""

    base_path: str = Field(description="The base path for the Parquet.")
    extension: str = Field(description="The file extension for the Parquet.", default="parquet")

    @property
    def _log(self) -> logging.Logger:
        return dagster.get_dagster_logger()

    def _get_fs_path(self, asset_key: AssetKey) -> str:
        rpath = f"{os.path.join(self.base_path, *asset_key.path)}.{self.extension}"
        return os.path.abspath(rpath)

    def handle_output(self, context, df: duckdb.DuckDBPyRelation):
        """This saves the dataframe as a parquet."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        self._log.info(f"Saving dataframe to '{fpath}'")
        df.to_parquet(fpath)

        context.add_output_metadata(
            {
                "dagster/row_count": MetadataValue.int(df.count("*").fetchall()[0][0]),
                "path": MetadataValue.path(fpath),
                # "sample": MetadataValue.md(df.display().to_markdown()),
                "dagster/column_schema": _get_dagster_table_schema(df),
            }
        )

    def load_input(self, context) -> duckdb.DuckDBPyRelation:
        """This reads a dataframe from a parquet."""
        fpath = self._get_fs_path(asset_key=context.asset_key)
        return duckdb.read_parquet(fpath, connection=duckdb.connect(":default:"))
