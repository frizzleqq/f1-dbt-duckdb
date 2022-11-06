import argparse
import contextlib
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
from pandas import DataFrame

import ergast

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
    parser = argparse.ArgumentParser(prog="DuckDB Staging", description="todo")
    parser.add_argument("table_name", nargs="?", default=None)
    group = parser.add_argument_group(
        "specific race arguments", "stage a specific race"
    )
    group.add_argument("season", nargs="?", default=None, type=int)
    group.add_argument("round", nargs="?", default=None, type=int)
    parser.add_argument("-r", "--read-full", action="store_true", default=None)
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
    table_name: str = None,
    season: int = None,
    race_round: int = None,
    read_full: bool = None,
) -> None:
    conn = duckdb_connect()

    if table_name is None:
        logging.info("Staging all tables.")
        for table_key, table in ergast.TABLES.items():
            logging.info(f"Staging {table_key}...")
            stage_table_from_dataframe(
                connection=conn,
                schema_name=table.schema_name,
                table_name=table.table_name,
                df=table.get_dataframe(
                    season=season, race_round=race_round, read_full=read_full
                ),
            )
    else:
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
            df=table.get_dataframe(
                season=season, race_round=race_round, read_full=read_full
            ),
        )

    # print_schema(conn, "stage_ergast")
    # print_schema(conn, "dm")


if __name__ == "__main__":
    arg_parser = get_parser()
    args = arg_parser.parse_args()

    # require all or none
    if any([args.season, args.round]) and not all([args.season, args.round]):
        raise argparse.ArgumentError(
            None, "Staging a specific race requires both season and round."
        )

    main(
        table_name=args.table_name,
        season=args.season,
        race_round=args.round,
        read_full=args.read_full,
    )
