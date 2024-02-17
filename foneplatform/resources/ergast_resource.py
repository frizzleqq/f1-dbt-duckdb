import datetime as dt
import itertools
import logging
from dataclasses import dataclass
from typing import Iterator, Tuple

import dagster
import pandas as pd
import requests
from pydantic import Field
from requests.adapters import HTTPAdapter, Retry


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


class ErgastResource(dagster.ConfigurableResource):  # type: ignore
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

    base_url: str = Field(
        description="Base url to Ergast API", default="http://ergast.com/api/f1"
    )
    min_season: int = Field(
        description="Oldest season to read from (to prevent inconsistencies in schema)",
        default=2000,
    )
    paging_size: int = Field(description="API paging size", default=400)

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
    def _extract_table_from_response(
        response: dict, response_path: Tuple[str, str]
    ) -> Iterator[dict]:
        table_key, list_key = response_path
        return (row for row in response["MRData"][table_key][list_key])

    @property
    def _log(self) -> logging.Logger:
        return dagster.get_dagster_logger()

    def _make_request(self, url: str, paging_size: int, offset: int = 0) -> dict:
        payload = {"limit": paging_size, "offset": offset}
        self._log.info(f"Requesting '{url}' with '{payload}'")
        response = get_request_session().get(url, params=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def _make_pagination(self, url: str):
        offset = 0
        result_size = self.paging_size
        while result_size > offset:
            content = self._make_request(url, self.paging_size, offset=offset)
            result_size = int(content["MRData"]["total"])
            yield content
            offset = offset + self.paging_size

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
        url = self._build_url(table, season=season, race_round=race_round)

        for response in self._make_pagination(url):
            yield response

    def request_table_last_race(self, table: str) -> Iterator[dict]:
        for row in self.request_table(table, season="current", race_round="last"):
            yield row

    def get_seasons_and_rounds(self, season: int | None = None):
        """
        Data of some tables can only be read by specifying season & round.
        So for older seasons we read each round of a season instead.

        Error: Bad Request: Lap time queries require a season and round to be specified

        :param season: if specified, only yield that season
        :return: tuple with (season, row)
        """
        response_generator = self.request_table("races")
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
        read_full_table: bool = False,
        read_all_seasons: bool = False,
    ) -> pd.DataFrame:
        if read_full_table:
            response_generator = self.request_table(table_name)
        elif season or read_all_seasons:
            generator_list = []
            for season, race_round in self.get_seasons_and_rounds(season=season):
                generator_list.append(
                    self.request_table(
                        table_name,
                        season=season,
                        race_round=race_round,
                    ),
                )
            response_generator = itertools.chain(*generator_list)
        else:
            response_generator = self.request_table_last_race(table_name)

        rows = []
        for response in response_generator:
            for row in self._extract_table_from_response(response, response_path):
                rows.append(row)

        df = pd.json_normalize(
            rows,
            record_path=record_path,
            meta=record_meta,
            sep="_",
            errors="ignore",
        )
        return df


class ErgastConfig(dagster.Config):  # type: ignore
    read_all_seasons: bool = False
    season: int = 0


@dataclass
class ErgastTable:
    table_name: str
    response_path: Tuple[str, str]
    record_path: str | list[str] | None = None
    record_meta: list | None = None
    always_full: bool = False


def build_ergast_asset(asset_name: str, table: ErgastTable):
    if table.always_full:
        # no config necessary
        @dagster.asset(name=asset_name, compute_kind="python")
        def asset_fn(ergast: ErgastResource) -> pd.DataFrame:
            df = ergast.get_dataframe(
                table.table_name,
                response_path=table.response_path,
                record_path=table.record_path,
                record_meta=table.record_meta,
                read_full_table=table.always_full,
            )
            df["load_dts"] = dt.datetime.now()
            return df

    else:

        @dagster.asset(name=asset_name, compute_kind="python")
        def asset_fn(ergast: ErgastResource, config: ErgastConfig) -> pd.DataFrame:
            df = ergast.get_dataframe(
                table.table_name,
                response_path=table.response_path,
                record_path=table.record_path,
                record_meta=table.record_meta,
                read_full_table=table.always_full,
                season=config.season,
                read_all_seasons=config.read_all_seasons,
            )
            df["load_dts"] = dt.datetime.now()
            return df

    return asset_fn
