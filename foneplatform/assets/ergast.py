from ..resources import ErgastResource

import pandas as pd

from dagster import asset


@asset()
def drivers(ergast: ErgastResource) -> pd.DataFrame:
    return ergast.get_dataframe(
        "drivers", response_path=("DriverTable", "Drivers"), read_full=True
    )


# TABLES = {
#     "drivers": ErgastTableReader(
#         table_name="drivers",
#         always_full=True,
#         response_path=("DriverTable", "Drivers"),
#     ),
#     "circuits": ErgastTableReader(
#         table_name="circuits",
#         always_full=True,
#         response_path=("CircuitTable", "Circuits"),
#     ),
#     "seasons": ErgastTableReader(
#         table_name="seasons",
#         always_full=True,
#         response_path=("SeasonTable", "Seasons"),
#     ),
#     "constructors": ErgastTableReader(
#         table_name="constructors",
#         always_full=True,
#         response_path=("ConstructorTable", "Constructors"),
#     ),
#     "races": ErgastTableReader(
#         table_name="races",
#         always_full=True,
#         response_path=("RaceTable", "Races"),
#     ),
#     "pitstops": ErgastTableReader(
#         table_name="pitstops",
#         always_full=False,
#         response_path=("RaceTable", "Races"),
#         record_path="PitStops",
#         record_meta=[
#             "season",
#             "round",
#             "date",
#             "raceName",
#             ["Circuit", "circuitId"],
#         ],
#     ),
#     "qualifying": ErgastTableReader(
#         table_name="qualifying",
#         always_full=False,
#         response_path=("RaceTable", "Races"),
#         record_path="QualifyingResults",
#         record_meta=[
#             "season",
#             "round",
#             "date",
#             "time",
#             "raceName",
#             "url",
#             ["Circuit", "circuitId"],
#         ],
#     ),
#     "results": ErgastTableReader(
#         table_name="results",
#         always_full=False,
#         response_path=("RaceTable", "Races"),
#         record_path="Results",
#         record_meta=[
#             "season",
#             "round",
#             "url",
#             "raceName",
#             "date",
#             "time",
#             ["Circuit", "circuitId"],
#         ],
#     ),
#     "laps": ErgastTableReader(
#         table_name="laps",
#         always_full=False,
#         response_path=("RaceTable", "Races"),
#         record_path=["Laps", "Timings"],
#         record_meta=[
#             "season",
#             "round",
#             "url",
#             "raceName",
#             "date",
#             "Circuit",
#             ["Laps", "number"],
#         ],
#     ),
#     "driverstandings": ErgastTableReader(
#         table_name="driverStandings",
#         always_full=False,
#         response_path=("StandingsTable", "StandingsLists"),
#         record_path="DriverStandings",
#         record_meta=[
#             "season",
#             "round",
#         ],
#     ),
#     "constructorstandings": ErgastTableReader(
#         table_name="constructorStandings",
#         always_full=False,
#         response_path=("StandingsTable", "StandingsLists"),
#         record_path="ConstructorStandings",
#         record_meta=[
#             "season",
#             "round",
#         ],
#     ),
# }
