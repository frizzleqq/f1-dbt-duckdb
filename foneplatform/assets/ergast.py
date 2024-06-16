import tempfile
import urllib
import zipfile
from pathlib import Path

import dagster
import duckdb

ergast_tables = {
    "circuits": dagster.AssetOut(is_required=False),
    "constructor_results": dagster.AssetOut(is_required=False),
    "constructor_standings": dagster.AssetOut(is_required=False),
    "constructors": dagster.AssetOut(is_required=False),
    "driver_standings": dagster.AssetOut(is_required=False),
    "drivers": dagster.AssetOut(is_required=False),
    "lap_times": dagster.AssetOut(is_required=False),
    "pit_stops": dagster.AssetOut(is_required=False),
    "qualifying": dagster.AssetOut(is_required=False),
    "races": dagster.AssetOut(is_required=False),
    "results": dagster.AssetOut(is_required=False),
    "seasons": dagster.AssetOut(is_required=False),
    "sprint_results": dagster.AssetOut(is_required=False),
    "status": dagster.AssetOut(is_required=False),
}


@dagster.multi_asset(outs=ergast_tables, can_subset=True, compute_kind="Python")
def download_ergast_image(context: dagster.AssetExecutionContext):
    db_con = duckdb.connect(":memory:")
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_path = Path(tmpdirname)
        zip_path = tmp_path / "f1db_csv.zip"
        urllib.request.urlretrieve("http://ergast.com/downloads/f1db_csv.zip", zip_path)
        with zipfile.ZipFile(zip_path, "r") as zip:
            for table in context.op_execution_context.selected_output_names:
                with zip.open(f"{table}.csv", "r") as f:
                    df = db_con.read_csv(
                        f,
                        header=True,
                        delimiter=",",
                        encoding="utf-8",
                        quotechar='"',
                        na_values=r"\N",
                    )
                    yield dagster.Output(df, output_name=table)
    db_con.close()
