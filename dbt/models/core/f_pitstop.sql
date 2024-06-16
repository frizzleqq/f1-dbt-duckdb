{{ config(materialized='external', format='parquet') }}

WITH pitstops AS (
    SELECT *
    FROM {{ ref('ergast_pit_stops') }}
)

, d_driver AS (
    SELECT *
    FROM {{ ref('d_driver') }}
)

, d_race AS (
    SELECT *
    FROM {{ ref('d_race') }}
)

, joined AS (
    SELECT
        d_race.race_key
        , d_race.race_date
        , d_driver.driver_key
        , pitstops.pitstop_number
        , pitstops.lap AS lap_number
        , d_race.race_date + pitstops.pitstop_time AS pitstop_timestamp
        , pitstops.duration AS pitstop_duration
        , CAST(pitstops.milliseconds AS DOUBLE) / 1000 AS pitstop_seconds
    FROM pitstops
    LEFT JOIN d_driver
        ON d_driver.driver_id = pitstops.driverid
    LEFT JOIN d_race
        ON d_race.race_id = pitstops.raceid
)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'race_key',
            'driver_key',
            'pitstop_number',
        ])
    }} AS pitstop_key
    , *
FROM joined
