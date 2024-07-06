{{ config(materialized='external', format='parquet') }}

WITH lap_times AS (
    SELECT *
    FROM {{ ref('ergast_lap_times') }}
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
        d_race.race_ref
        , d_race.race_date
        , d_driver.driver_ref
        , lap_times.lap AS lap_number
        , lap_times.race_position
        , lap_times.lap_time
        , lap_times.lap_milliseconds
    FROM lap_times
    LEFT JOIN d_driver
        ON d_driver.driver_id = lap_times.driverid
    LEFT JOIN d_race
        ON d_race.race_id = lap_times.raceid
)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'race_ref',
            'driver_ref',
            'lap_number',
        ])
    }} AS lap_ref
    , *
FROM joined
