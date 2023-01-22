import argparse
import contextlib
import logging
import os
from pathlib import Path

import duckdb
import ergast
import pandas as pd
from pandas import DataFrame

# defaults to <project-root>/data
DUCKDB_DIR = Path(
    os.environ.get(
        "DUCKDB_DIR",
        Path(__file__).parent.parent.joinpath("data"),
    )
)


logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%d-%m-%y %H:%M:%S", level=logging.INFO
)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load data from Ergast WebService into DuckDB, "
        "by default only the last race is loaded."
    )
    parser.add_argument(
        "table_names",
        nargs="*",
        default=[],
        help="List of table names to load, omit to load all tables",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-s",
        "--season",
        default=None,
        type=int,
        help=f"Read provided season (=year) fully for fact tables.",
    )
    group.add_argument(
        "-r",
        "--read-full",
        default=None,
        action="store_true",
        help=f"Read past seasons (beginning with {ergast.MIN_SEASON}) for fact tables.",
    )
    return parser


def duckdb_connect(duckdb_dir: Path = None) -> duckdb.DuckDBPyConnection:
    if duckdb_dir is None:
        duckdb_dir = DUCKDB_DIR
    duckdb_dir.mkdir(exist_ok=True)
    return duckdb.connect(
        database=str(duckdb_dir.joinpath("f1.duckdb")), read_only=False
    )


@contextlib.contextmanager
def duckdb_cursor(connection: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
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
    with duckdb_cursor(connection) as cursor:
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
    with duckdb_cursor(connection) as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def create_table_as_dataframe(
    connection: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    tmp_view = f"tmp_{schema_name}_{table_name}"
    with duckdb_cursor(connection) as cursor:
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
    return count


def main(
    table_names: list = None,
    season: int = None,
    read_full: bool = None,
) -> None:
    conn = duckdb_connect()

    if not table_names:
        table_names = ergast.TABLES.keys()

    for table_name in table_names:

        try:
            table = ergast.TABLES[table_name]
        except KeyError:
            print(f"Available tables: {list(ergast.TABLES.keys())}.")
            raise

        logging.info(f"Staging {table_name}...")
        stage_table_from_dataframe(
            connection=conn,
            schema_name=table.schema_name,
            table_name=table.table_name,
            df=table.get_dataframe(season=season, read_full=read_full),
        )


if __name__ == "__main__":
    arg_parser = get_parser()
    args = arg_parser.parse_args()

    main(
        table_names=args.table_names,
        season=args.season,
        read_full=args.read_full,
    )
