{{ config(materialized = 'view') }}

SELECT
    CAST("number" AS INT) AS qualifying_number
    , CAST("position" AS INT) AS qualifying_position
    , driver_driverid
    , constructor_constructorid
    , circuit_circuitid
    , q1
    , q2
    , q3
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , CAST("date" AS DATE) AS qualifying_date
    , CAST("time" AS TIME) AS qualifying_time
    , racename
    , url
    , load_dts
FROM {{ source('ergast', 'qualifying') }}
