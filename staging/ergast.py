from dataclasses import dataclass
from typing import Iterator, Union, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

"""
import ergast

# Dataframe
list(ergast_table.get_dataframe())
# Dataframe (always full history)
list(ergast_table.get_dataframe(read_full=True))

# JSON (API) full history
list(ergast.Ergast.read_table("drivers")
# JSON (API) specific event/year
list(ergast.Ergast.read_table("drivers", year=2017, race=5))
list(ergast.Ergast.read_table_current_year("drivers"))
list(ergast.Ergast.read_table_last_race("drivers"))
"""

str_or_int = Union[str, int]


@dataclass
class Table:
    schema_name: str
    table_name: str
    response_path: Tuple
    column_mapping: dict = None
    read_full: bool = False

    def get_dataframe(self, read_full: bool = None) -> pd.DataFrame:
        if read_full is None:
            read_full = self.read_full
        if read_full:
            df = pd.DataFrame(
                list(ErgastAPI.read_table(self.table_name, self.response_path))
            )
        else:
            df = pd.DataFrame(
                list(
                    ErgastAPI.read_table_last_race(self.table_name, self.response_path)
                )
            )
        if self.column_mapping:
            df = df.astype(self.column_mapping)
        return df


TABLES = [
    Table(
        schema_name="stage_ergast",
        table_name="drivers",
        read_full=True,
        response_path=("DriverTable", "Drivers"),
        column_mapping={
            "driverId": str,
            "url": str,
            "givenName": str,
            "familyName": str,
            "dateOfBirth": "datetime64[ns]",
            "nationality": str,
            "permanentNumber": "Int16",
            "code": str,
        },
    ),
    Table(
        schema_name="stage_ergast",
        table_name="circuits",
        read_full=True,
        response_path=("CircuitTable", "Circuits"),
        column_mapping={
            "circuitId": str,
            "url": str,
            "circuitName": str,
            "Location": str,
        },
    ),
    Table(
        schema_name="stage_ergast",
        table_name="seasons",
        read_full=True,
        response_path=("SeasonTable", "Seasons"),
        # column_mapping={"circuitId": str, "url": str, "circuitName": str, "Location": str},
    ),
    Table(
        schema_name="stage_ergast",
        table_name="constructors",
        read_full=True,
        response_path=("ConstructorTable", "Constructors"),
        # column_mapping={"circuitId": str, "url": str, "circuitName": str, "Location": str},
    ),
    Table(
        schema_name="stage_ergast",
        table_name="qualifying",
        read_full=False,
        response_path=("RaceTable", "Races"),
        # column_mapping={"circuitId": str, "url": str, "circuitName": str, "Location": str},
    ),
    Table(
        schema_name="stage_ergast",
        table_name="results",
        read_full=False,
        response_path=("RaceTable", "Races"),
        # column_mapping={"circuitId": str, "url": str, "circuitName": str, "Location": str},
    ),
    Table(
        schema_name="stage_ergast",
        table_name="laps",
        read_full=False,
        response_path=("RaceTable", "Races"),
        # column_mapping={"circuitId": str, "url": str, "circuitName": str, "Location": str},
    ),
]


class ErgastAPI:
    base_url = "http://ergast.com/api/f1"
    response_format = "json"
    paging = 100

    retry_strategy = Retry(
        total=3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    @staticmethod
    def _build_url(table: str, year: str = None, race: str = None) -> str:
        url_parts = [ErgastAPI.base_url]
        if year:
            url_parts.append(year)
        if race:
            url_parts.append(race)
        url_parts.append(f"{table}.{ErgastAPI.response_format}")
        return "/".join(url_parts)

    @staticmethod
    def request_response(url: str, offset: int = 0) -> dict:
        payload = {"limit": ErgastAPI.paging, "offset": offset}
        response = ErgastAPI.http.get(url, params=payload)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _response_paging(url):
        offset = 0
        result_size = ErgastAPI.paging
        while result_size > offset:
            content = ErgastAPI.request_response(url, offset=offset)
            result_size = int(content["MRData"]["total"])
            yield content
            offset = offset + ErgastAPI.paging

    @staticmethod
    def _extract_table_from_response(response: dict, response_path: Tuple) -> list:
        table_key, list_key = response_path
        return response["MRData"][table_key][list_key]

    @staticmethod
    def read_table(
        table: str,
        response_path: Tuple = None,
        year: str_or_int = None,
        race: str_or_int = None,
    ) -> Iterator[dict]:
        if year is None and race is not None:
            raise ValueError("Cannot get race without specifying year.")
        if year is not None:
            year = str(year)
        if race is not None:
            race = str(race)
        url = ErgastAPI._build_url(table, year=year, race=race)

        for response in ErgastAPI._response_paging(url):
            if response_path is None:
                yield response
            else:
                for row in ErgastAPI._extract_table_from_response(
                    response, response_path
                ):
                    yield row

    @staticmethod
    def read_table_current_year(
        table: str, response_path: Tuple = None
    ) -> Iterator[dict]:
        for row in ErgastAPI.read_table(table, response_path, year="current"):
            yield row

    @staticmethod
    def read_table_last_race(table: str, response_path: Tuple = None) -> Iterator[dict]:
        for row in ErgastAPI.read_table(
            table, response_path, year="current", race="last"
        ):
            yield row
