{{ config(materialized = 'view') }}

SELECT
    "number"
    , "position"
    , driver_driverid
    , constructor_constructorid
    , circuit_circuitid
    , q1
    , q2
    , q3
    , season
    , round
    , "date"
    , "time"
    , racename
    , url
    , load_dts
FROM {{ source('ergast', 'qualifying') }}
