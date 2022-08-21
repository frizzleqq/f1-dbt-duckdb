from typing import Iterator, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

"""
from ergast import Ergast

list(Ergast.read_table("drivers"))
list(Ergast.read_table("drivers", year=2017, race=5))
list(Ergast.read_table_last_race("drivers", year=2020))
list(Ergast.read_table_last_race("drivers"))
"""

str_or_int = Union[str, int]


table_column_types = {
    "drivers": {
        "driverId": str,
        "url": str,
        "givenName": str,
        "familyName": str,
        "dateOfBirth": "datetime64[ns]",
        "nationality": str,
        "permanentNumber": "Int16",
        "code": str,
    },
    "circuits": {"circuitId": str, "url": str, "circuitName": str, "Location": str},
}


def read_table(table_name: str, read_full: bool = False) -> pd.DataFrame:
    if read_full:
        df = pd.DataFrame(list(ErgastAPI.read_table(table_name)))
    else:
        df = pd.DataFrame(list(ErgastAPI.read_table_last_race(table_name)))
    mapping = table_column_types.get(table_name, None)
    if mapping:
        df = df.astype(mapping)
    return df


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
    def _extract_table_from_response(response: dict, table: str):
        tables_mapping = {
            "drivers": ("DriverTable", "Drivers"),
            "circuits": ("CircuitTable", "Circuits"),
        }
        table_key, list_key = tables_mapping[table]
        return response["MRData"][table_key][list_key]

    @staticmethod
    def read_table(
        table: str, year: str_or_int = None, race: str_or_int = None
    ) -> Iterator[dict]:
        if year is None and race is not None:
            raise ValueError("Cannot get race without specifying year.")
        if year is not None:
            year = str(year)
        if race is not None:
            race = str(race)
        url = ErgastAPI._build_url(table, year=year, race=race)

        for response in ErgastAPI._response_paging(url):
            for row in ErgastAPI._extract_table_from_response(response, table):
                yield row

    @staticmethod
    def read_table_current_year(table: str) -> Iterator[dict]:
        for row in ErgastAPI.read_table(table, year="current"):
            yield row

    @staticmethod
    def read_table_last_race(table: str) -> Iterator[dict]:
        for row in ErgastAPI.read_table(table, year="current", race="last"):
            yield row
