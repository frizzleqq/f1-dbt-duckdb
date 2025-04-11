import logging
import os
from typing import Sequence

import dagster
import duckdb
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    InputContext,
    MetadataValue,
    OutputContext,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_duckdb.io_manager import DuckDbClient, DuckDBIOManager
from pydantic import Field


def _get_dagster_table_schema(df: duckdb.DuckDBPyRelation) -> dagster.TableSchema:
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


class DuckDBDuckDBTypeHandler(DbTypeHandler[duckdb.DuckDBPyRelation]):  # type: ignore
    """Stores and loads Pandas DataFrames in DuckDB.

    To use this type handler, return it from the ``type_handlers` method of an I/O manager that inherits from ``DuckDBIOManager``.
    """

    @staticmethod
    def table_name(table_slice: TableSlice) -> str:
        """Returns the name of the table in the database."""
        return f"{table_slice.schema}.{table_slice.table}"

    def handle_output(
        self,
        context: OutputContext,
        table_slice: TableSlice,
        obj: duckdb.DuckDBPyRelation,
        connection: duckdb.DuckDBPyConnection,
    ):
        """Stores the duckdb relation in duckdb."""
        try:
            obj.create(DuckDBDuckDBTypeHandler.table_name(table_slice))
        except duckdb.CatalogException:
            DuckDbClient.delete_table_slice(context, table_slice, connection)
            obj.insert_into(DuckDBDuckDBTypeHandler.table_name(table_slice))

        # https://docs.dagster.io/guides/build/assets/metadata-and-tags
        context.add_output_metadata(
            {
                "dagster/row_count": MetadataValue.int(obj.count("*").fetchall()[0][0]),
                "dagster/column_schema": MetadataValue.table_schema(_get_dagster_table_schema(obj)),
            }
        )

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection: duckdb.DuckDBPyConnection
    ) -> duckdb.DuckDBPyRelation:
        """Loads the input as a Pandas DataFrame."""
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return duckdb.DuckDBPyRelation()
        return connection.query(DuckDbClient.get_select_statement(table_slice))

    @property
    def supported_types(self):
        return [duckdb.DuckDBPyRelation]


class DuckDBDuckDBIOManager(DuckDBIOManager):
    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [DuckDBDuckDBTypeHandler()]
