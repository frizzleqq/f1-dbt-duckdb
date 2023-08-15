import itertools
from dataclasses import dataclass
from typing import Iterator, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

"""
# ErgastTableReader
table_reader = ErgastTableReader(
        table_name="drivers",
        always_full=True,
        response_path=("DriverTable", "Drivers"),
    )
list(table_reader.get_dataframe())
list(table_reader.get_dataframe(season=2017))
list(table_reader.get_dataframe(read_full=True))

# JSON (API) full history
list(ErgastAPI.read_table("drivers")
# JSON (API) specific event/year
list(ErgastAPI.read_table("drivers", year=2017, race_round=5))
list(ErgastAPI.read_table_current_year("drivers"))
list(ErgastAPI.read_table_last_race("drivers"))
"""

MIN_SEASON = 2000


@dataclass
class ErgastTableReader:
    table_name: str
    response_path: Tuple
    record_path: str | list[str] | None = None
    record_meta: list | None = None
    always_full: bool = False

    def _extract_table_from_response(self, response) -> Iterator[dict]:
        table_key, list_key = self.response_path
        return (row for row in response["MRData"][table_key][list_key])

    def _get_seasons_and_rounds(self, season: int | None = None):
        """
        Data of some tables can only be read by specifying season & round.
        So for older seasons we read each round of a season instead.

        Error: Bad Request: Lap time queries require a season and round to be specified

        :param season: if specified, only yield that season
        :return: tuple with (season, row)
        """
        response_generator = ErgastAPI.read_table("races")
        for response in response_generator:
            for row in self._extract_table_from_response(response):
                row_season = int(row["season"])
                if season:
                    if row_season == season:
                        yield row["season"], row["round"]
                elif row_season >= MIN_SEASON:
                    yield row["season"], row["round"]

    def get_dataframe(
        self, season: int | None = None, read_full: bool = False
    ) -> pd.DataFrame:
        if self.always_full:
            response_generator = ErgastAPI.read_table(self.table_name)
        elif season or read_full:
            generator_list = []
            for season, race_round in self._get_seasons_and_rounds(season=season):
                generator_list.append(
                    ErgastAPI.read_table(
                        self.table_name,
                        season=season,
                        race_round=race_round,
                        paging_size=500,
                    ),
                )
            response_generator = itertools.chain(*generator_list)
        else:
            response_generator = ErgastAPI.read_table_last_race(self.table_name)

        rows = []
        for response in response_generator:
            for row in self._extract_table_from_response(response):
                rows.append(row)

        df = pd.json_normalize(
            rows,
            record_path=self.record_path,
            meta=self.record_meta,
            sep="_",
            errors="ignore",
        )
        return df


TABLES = {
    "drivers": ErgastTableReader(
        table_name="drivers",
        always_full=True,
        response_path=("DriverTable", "Drivers"),
    ),
    "circuits": ErgastTableReader(
        table_name="circuits",
        always_full=True,
        response_path=("CircuitTable", "Circuits"),
    ),
    "seasons": ErgastTableReader(
        table_name="seasons",
        always_full=True,
        response_path=("SeasonTable", "Seasons"),
    ),
    "constructors": ErgastTableReader(
        table_name="constructors",
        always_full=True,
        response_path=("ConstructorTable", "Constructors"),
    ),
    "races": ErgastTableReader(
        table_name="races",
        always_full=True,
        response_path=("RaceTable", "Races"),
    ),
    "pitstops": ErgastTableReader(
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
    "qualifying": ErgastTableReader(
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
    "results": ErgastTableReader(
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
    "laps": ErgastTableReader(
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
            ["Laps", "number"],
        ],
    ),
}


class ErgastAPI:
    base_url = "http://ergast.com/api/f1"
    response_format = "json"

    retry_strategy = Retry(
        total=3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    @staticmethod
    def _build_url(
        table: str, season: str | None = None, race_round: str | None = None
    ) -> str:
        url_parts = [ErgastAPI.base_url]
        if season:
            url_parts.append(season)
        if race_round:
            url_parts.append(race_round)
        url_parts.append(f"{table}.{ErgastAPI.response_format}")
        return "/".join(url_parts)

    @staticmethod
    def _request_response(url: str, paging_size: int, offset: int = 0) -> dict:
        payload = {"limit": paging_size, "offset": offset}
        response = ErgastAPI.http.get(url, params=payload)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _response_paging(url: str, paging_size: int):
        offset = 0
        result_size = paging_size
        while result_size > offset:
            content = ErgastAPI._request_response(url, paging_size, offset=offset)
            result_size = int(content["MRData"]["total"])
            yield content
            offset = offset + paging_size

    @staticmethod
    def read_table(
        table: str,
        season: str | int | None = None,
        race_round: str | int | None = None,
        paging_size: int = 100,
    ) -> Iterator[dict]:
        if season is None and race_round is not None:
            raise ValueError("Cannot get race_round without specifying year.")
        if season is not None:
            season = str(season)
        if race_round is not None:
            race_round = str(race_round)
        url = ErgastAPI._build_url(table, season=season, race_round=race_round)

        for response in ErgastAPI._response_paging(url, paging_size):
            yield response

    @staticmethod
    def read_table_current_year(table: str) -> Iterator[dict]:
        for row in ErgastAPI.read_table(table, season="current"):
            yield row

    @staticmethod
    def read_table_last_race(table: str) -> Iterator[dict]:
        for row in ErgastAPI.read_table(table, season="current", race_round="last"):
            yield row
