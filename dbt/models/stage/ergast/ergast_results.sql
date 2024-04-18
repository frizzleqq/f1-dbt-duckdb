{{ config(materialized = 'view') }}

SELECT
    CAST(position AS INT) AS result_position
    , positiontext
    , CAST(points AS DOUBLE) AS points
    , CAST(grid AS INT) AS grid
    , CAST(laps AS INT) AS laps
    , status AS result_status
    , driver_driverid
    , constructor_constructorid
    , CAST(time_millis AS INT) AS time_millis
    , time_time
    , CAST(fastestlap_rank AS INT) AS fastestlap_rank
    , CAST(fastestlap_lap AS INT) AS fastestlap_lap
    , fastestlap_time_time
    , fastestlap_averagespeed_units
    , CAST(fastestlap_averagespeed_speed AS DOUBLE) AS fastestlap_averagespeed_speed
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , CAST(date AS DATE) AS race_date
    , circuit_circuitid
    , load_dts
FROM {{ source('ergast', 'results') }}
