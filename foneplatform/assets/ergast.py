from ..resources.ergast_resource import ErgastTable, build_ergast_asset

TABLES = {
    "drivers": ErgastTable(
        table_name="drivers",
        always_full=True,
        response_path=("DriverTable", "Drivers"),
    ),
    "circuits": ErgastTable(
        table_name="circuits",
        always_full=True,
        response_path=("CircuitTable", "Circuits"),
    ),
    "seasons": ErgastTable(
        table_name="seasons",
        always_full=True,
        response_path=("SeasonTable", "Seasons"),
    ),
    "constructors": ErgastTable(
        table_name="constructors",
        always_full=True,
        response_path=("ConstructorTable", "Constructors"),
    ),
    "races": ErgastTable(
        table_name="races",
        always_full=True,
        response_path=("RaceTable", "Races"),
    ),
    "pitstops": ErgastTable(
        table_name="pitstops",
        always_full=False,
        response_path=("RaceTable", "Races"),
        record_path="PitStops",
        record_meta=[
            "season",
            "round",
            "date",
            "raceName",
            ["Circuit", "circuitId"],
        ],
    ),
    "qualifying": ErgastTable(
        table_name="qualifying",
        always_full=False,
        response_path=("RaceTable", "Races"),
        record_path="QualifyingResults",
        record_meta=[
            "season",
            "round",
            "date",
            "time",
            "raceName",
            "url",
            ["Circuit", "circuitId"],
        ],
    ),
    "results": ErgastTable(
        table_name="results",
        always_full=False,
        response_path=("RaceTable", "Races"),
        record_path="Results",
        record_meta=[
            "season",
            "round",
            "url",
            "raceName",
            "date",
            "time",
            ["Circuit", "circuitId"],
        ],
    ),
    "laps": ErgastTable(
        table_name="laps",
        always_full=False,
        response_path=("RaceTable", "Races"),
        record_path=["Laps", "Timings"],
        record_meta=[
            "season",
            "round",
            "url",
            "raceName",
            "date",
            "Circuit",
            ["Laps", "number"],
        ],
    ),
    "driverStandings": ErgastTable(
        table_name="driverStandings",
        always_full=False,
        response_path=("StandingsTable", "StandingsLists"),
        record_path="DriverStandings",
        record_meta=[
            "season",
            "round",
        ],
    ),
    "constructorStandings": ErgastTable(
        table_name="constructorStandings",
        always_full=False,
        response_path=("StandingsTable", "StandingsLists"),
        record_path="ConstructorStandings",
        record_meta=[
            "season",
            "round",
        ],
    ),
}

ergast_assets = [
    build_ergast_asset(table_name, table) for table_name, table in TABLES.items()
]
