import contextlib
import logging
from pathlib import Path

import duckdb
import pandas as pd
from pandas import DataFrame

import ergast

ROOT_DIR = Path(__file__).parent.parent
DB_DIR = ROOT_DIR.joinpath("data")
DB_PATH = DB_DIR.joinpath("f1.duckdb")


logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%d-%m-%y %H:%M:%S", level=logging.INFO
)


@contextlib.contextmanager
def cursor_duckdb(connection: duckdb.DuckDBPyConnection):
    cursor = connection.cursor()
    cursor.begin()
    try:
        yield cursor
    except Exception as e:
        logging.error(e)
        cursor.rollback()
        raise
    else:
        cursor.commit()
    finally:
        cursor.close()


def print_schema(connection: duckdb.DuckDBPyConnection, schema_name: str) -> None:
    with cursor_duckdb(connection) as cursor:
        for row in cursor.execute(
            f"""
            SELECT t.table_schema, t.table_name, t.table_type, c.ordinal_position, c.column_name, c.data_type
            FROM information_schema.tables as t
            INNER JOIN information_schema.columns as c on c.table_schema = t.table_schema
                and c.table_name = t.table_name
            WHERE t.table_schema = '{schema_name}'
            ORDER BY t.table_schema, t.table_name, c.ordinal_position
        """
        ).fetchall():
            print(row)


def create_schema(connection: duckdb.DuckDBPyConnection, schema_name: str) -> None:
    with cursor_duckdb(connection) as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def create_table_as_dataframe(
    connection: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    tmp_view = f"tmp_{schema_name}_{table_name}"
    with cursor_duckdb(connection) as cursor:
        cursor.register(tmp_view, df)
        cursor.execute(
            f"""CREATE OR REPLACE TABLE {schema_name}.{table_name} AS
                SELECT *
                    , now() AS load_dts
                FROM {tmp_view}"""
        )
        return cursor.fetchall()[0][0]


def stage_table_from_dataframe(
    connection: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
    df: DataFrame,
):
    create_schema(connection, schema_name)
    logging.info(f"Loading into '{schema_name}.{table_name}'...")
    count = create_table_as_dataframe(connection, schema_name, table_name, df)
    logging.info(f"Loaded {count} rows into '{schema_name}.{table_name}'.")


def main() -> None:
    DB_DIR.mkdir(exist_ok=True)
    conn = duckdb.connect(database=str(DB_PATH), read_only=False)

    for table in ergast.TABLES:
        stage_table_from_dataframe(
            connection=conn,
            schema_name=table.schema_name,
            table_name=table.table_name,
            df=table.get_dataframe(read_full=None),
        )

    print_schema(conn, "stage_ergast")
    print_schema(conn, "dm")


if __name__ == "__main__":
    main()
