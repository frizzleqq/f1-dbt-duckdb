{{ config(materialized='external', format='parquet') }}

WITH circuits AS (
    SELECT *
    FROM {{ ref('ergast_circuits') }}
)

, races AS (
    SELECT *
    FROM {{ ref('ergast_races') }}
)

SELECT
    races.raceid AS race_id
    , concat(races.season, '-', races.round) AS race_key
    , races.season AS race_season
    , races.round AS race_round
    , circuits.circuitref AS circuit_key
    , races.race_name
    , races.url AS race_url
    , races.race_date
    , races.race_date + race_time AS race_timestamp
    , races.fp1_date
    , races.fp1_date + fp1_time AS fp1_timestamp
    , races.fp2_date
    , races.fp2_date + fp2_time AS fp2_timestamp
    , races.fp3_date
    , races.fp3_date + fp3_time AS fp3_timestamp
    , races.quali_date AS qualifying_date
    , races.quali_date + quali_time AS qualifying_timestamp
    , races.sprint_date
    , races.sprint_date + sprint_time AS sprint_timestamp
FROM races
LEFT JOIN circuits ON circuits.circuitid = races.circuitid
