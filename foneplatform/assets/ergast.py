import io
import tempfile
import time
import urllib
import urllib.request
import zipfile
from http.client import HTTPMessage
from pathlib import Path

import dagster
from dagster_duckdb import DuckDBResource

ERGAST_URI = "http://ergast.com/downloads/f1db_csv.zip"

_metadata = {"dagster/uri": ERGAST_URI}

ergast_tables = {
    "circuits": dagster.AssetOut(is_required=False, metadata=_metadata),
    "constructor_results": dagster.AssetOut(is_required=False, metadata=_metadata),
    "constructor_standings": dagster.AssetOut(is_required=False, metadata=_metadata),
    "constructors": dagster.AssetOut(is_required=False, metadata=_metadata),
    "driver_standings": dagster.AssetOut(is_required=False, metadata=_metadata),
    "drivers": dagster.AssetOut(is_required=False, metadata=_metadata),
    "lap_times": dagster.AssetOut(is_required=False, metadata=_metadata),
    "pit_stops": dagster.AssetOut(is_required=False, metadata=_metadata),
    "qualifying": dagster.AssetOut(is_required=False, metadata=_metadata),
    "races": dagster.AssetOut(is_required=False, metadata=_metadata),
    "results": dagster.AssetOut(is_required=False, metadata=_metadata),
    "seasons": dagster.AssetOut(is_required=False, metadata=_metadata),
    "sprint_results": dagster.AssetOut(is_required=False, metadata=_metadata),
    "status": dagster.AssetOut(is_required=False, metadata=_metadata),
}


def url_retrieve(
    url: str, filename: Path, retries: int = 1, retry_delay=3
) -> tuple[str, HTTPMessage]:
    """Download a file from a URL to a local file, with retries."""
    for attempt in range(1 + retries):
        try:
            return urllib.request.urlretrieve(url, filename)
        except urllib.error.URLError as e:
            if attempt < retries:
                time.sleep(retry_delay)
            else:
                raise e
    raise ValueError(f"retries must be a non-negative integer, got {retries}")


@dagster.multi_asset(outs=ergast_tables, can_subset=True, compute_kind="DuckDB")
def download_ergast_image(context: dagster.AssetExecutionContext, duckdb_resource: DuckDBResource):
    with duckdb_resource.get_connection() as db_con:
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_path = Path(tmpdirname)
            zip_path = tmp_path / "f1db_csv.zip"
            url_retrieve(ERGAST_URI, zip_path)
            with zipfile.ZipFile(zip_path, "r") as zip:
                for table in context.op_execution_context.selected_output_names:
                    with zip.open(f"{table}.csv", "r") as f:
                        df = db_con.read_csv(
                            io.TextIOWrapper(f, encoding="utf-8"),
                            header=True,
                            delimiter=",",
                            encoding="utf-8",
                            quotechar='"',
                            na_values=r"\N",
                        )
                        yield dagster.Output(df, output_name=table)
