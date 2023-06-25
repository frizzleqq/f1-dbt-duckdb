import argparse
import datetime as dt
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from foneload import ergast

# defaults to <project-root>/data
DUCKDB_DIR = Path(
    os.environ.get(
        "DUCKDB_DIR",
        Path(__file__).parent.parent.joinpath("data"),
    )
)


@dataclass
class StageResult:
    file_path: Path
    row_count: int


logging.basicConfig(
    format="%(asctime)s - %(message)s", datefmt="%d-%m-%y %H:%M:%S", level=logging.INFO
)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load data from Ergast WebService into CSV files (for DuckDB), "
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
        help="Read provided season (=year) fully for fact tables.",
    )
    group.add_argument(
        "-r",
        "--read-full",
        default=None,
        action="store_true",
        help=f"Read past seasons (beginning with {ergast.MIN_SEASON}) for fact tables.",
    )
    return parser


def stage_table(
    table_reader: ergast.TableReader,
    season: Optional[int] = None,
    read_full: bool = False,
) -> StageResult:
    raw_dir = DUCKDB_DIR.joinpath("raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    file_path = raw_dir.joinpath(f"{table_reader.table_name}.csv")
    df = table_reader.get_dataframe(season=season, read_full=read_full)
    df["load_dts"] = dt.datetime.now()
    df.to_csv(file_path, index=False, mode="w")
    return StageResult(file_path, len(df.index))


def run(
    table_names: list,
    season: Optional[int] = None,
    read_full: bool = False,
) -> None:
    if not table_names:
        table_names = list(ergast.TABLES.keys())

    for table_name in table_names:
        try:
            table_reader = ergast.TABLES[table_name]
        except KeyError:
            print(f"Available tables: {list(ergast.TABLES.keys())}.")
            raise

        logging.info(f"Staging {table_reader.table_name}...")
        stage_result = stage_table(
            table_reader,
            season=season,
            read_full=read_full,
        )
        logging.info(
            f"Staged {table_reader.table_name} ({stage_result.row_count} rows)"
            f" into '{stage_result.file_path}'."
        )


def main():
    arg_parser = get_parser()
    args = arg_parser.parse_args()

    run(
        table_names=args.table_names,
        season=args.season,
        read_full=args.read_full,
    )


if __name__ == "__main__":
    main()
