from typing import Sequence

import dagster
import duckdb
from dagster import (
    InputContext,
    MetadataValue,
    OutputContext,
)
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster_duckdb.io_manager import DuckDbClient, DuckDBIOManager


def _get_dagster_table_schema(df: duckdb.DuckDBPyRelation) -> dagster.TableSchema:
    """Returns a TableSchema for the given duckdb dataframe."""

    return dagster.TableSchema(
        columns=[dagster.TableColumn(name=col[0], type=col[1]) for col in df.description]
    )


class DuckDBDuckDBTypeHandler(DbTypeHandler[duckdb.DuckDBPyRelation]):  # type: ignore
    """Stores and loads DuckdDB-Relations in DuckDB.

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
