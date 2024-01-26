import itertools
from typing import Iterator, Tuple

from dagster import ConfigurableResource
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from pydantic import Field


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


def get_request_session(request_max_retries: int = 3) -> requests.Session:
    retry_strategy = Retry(
        total=request_max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


class ErgastResource(ConfigurableResource):
    """
    # Get pandas dataframe
    ergast_resource = ErgastResource(
        table_name="drivers",
        always_full=True,
        response_path=("DriverTable", "Drivers"),
    )
    list(ergast_resource.get_dataframe())
    list(ergast_resource.get_dataframe(season=2017))
    list(ergast_resource.get_dataframe(read_full=True))

    # JSON (API) full history
    list(ErgastAPI.read_table("drivers")
    # JSON (API) specific event/year
    list(ErgastAPI.read_table("drivers", year=2017, race_round=5))
    list(ErgastAPI.read_table_current_year("drivers"))
    list(ErgastAPI.read_table_last_race("drivers"))
    """

    base_url: str = Field(description="TODO", default="http://ergast.com/api/f1")
    min_season: int = Field(description="TODO", default=2000)
    paging_size: int = Field(description="TODO", default=100)
    paging_size_big: int = Field(description="TODO", default=500)

    def _build_url(
        self, table: str, season: str | None = None, race_round: str | None = None
    ) -> str:
        url_parts = [self.base_url]
        if season:
            url_parts.append(season)
        if race_round:
            url_parts.append(race_round)
        url_parts.append(f"{table}.json")
        return "/".join(url_parts)

    @staticmethod
    def _request_response(url: str, paging_size: int, offset: int = 0) -> dict:
        payload = {"limit": paging_size, "offset": offset}
        # TODO: get new instance instead
        response = get_request_session().get(url, params=payload, timout=30)
        response.raise_for_status()
        return response.json()

    def _response_paging(self, url: str):
        offset = 0
        result_size = self.paging_size
        while result_size > offset:
            content = ErgastResource._request_response(
                url, self.paging_size, offset=offset
            )
            result_size = int(content["MRData"]["total"])
            yield content
            offset = offset + self.paging_size

    @staticmethod
    def _extract_table_from_response(
        response: dict, response_path: Tuple[str, str]
    ) -> Iterator[dict]:
        table_key, list_key = response_path
        return (row for row in response["MRData"][table_key][list_key])

    @staticmethod
    def request_table(
        self,
        table: str,
        season: str | int | None = None,
        race_round: str | int | None = None,
    ) -> Iterator[dict]:
        if season is None and race_round is not None:
            raise ValueError("Cannot get race_round without specifying year.")
        if season is not None:
            season = str(season)
        if race_round is not None:
            race_round = str(race_round)
        url = ErgastResource._build_url(table, season=season, race_round=race_round)

        for response in ErgastResource._response_paging(url):
            yield response

    @staticmethod
    def request_table_last_race(table: str) -> Iterator[dict]:
        for row in ErgastResource.read_table(
            table, season="current", race_round="last"
        ):
            yield row

    def get_seasons_and_rounds(self, season: int | None = None):
        """
        Data of some tables can only be read by specifying season & round.
        So for older seasons we read each round of a season instead.

        Error: Bad Request: Lap time queries require a season and round to be specified

        :param season: if specified, only yield that season
        :return: tuple with (season, row)
        """
        response_generator = ErgastResource.request_table("races")
        response_path = ("RaceTable", "Races")
        for response in response_generator:
            for row in self._extract_table_from_response(response, response_path):
                row_season = int(row["season"])
                if season:
                    if row_season == season:
                        yield row["season"], row["round"]
                elif row_season >= self.min_season:
                    yield row["season"], row["round"]

    def get_dataframe(
        self,
        table_name: str,
        response_path: Tuple[str, str],
        record_path: str | list[str] | None = None,
        record_meta: list | None = None,
        season: int | None = None,
        read_full: bool = False,
    ) -> pd.DataFrame:
        if season or read_full:
            generator_list = []
            for season, race_round in self.get_seasons_and_rounds(season=season):
                generator_list.append(
                    ErgastResource.request_table(
                        self.table_name,
                        season=season,
                        race_round=race_round,
                        paging_size=self.paging_size_big,
                    ),
                )
            response_generator = itertools.chain(*generator_list)
        else:
            response_generator = ErgastResource.request_table_last_race(self.table_name)

        rows = []
        for response in response_generator:
            for row in self._extract_table_from_response(response, self.response_path):
                rows.append(row)

        df = pd.json_normalize(
            rows,
            record_path=self.record_path,
            meta=self.record_meta,
            sep="_",
            errors="ignore",
        )
        return df
