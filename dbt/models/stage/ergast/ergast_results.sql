{{ config(materialized = 'view') }}

SELECT
    "position"
    , positiontext
    , points
    , grid
    , laps
    , status
    , driver_driverid
    , constructor_constructorid
    , time_millis
    , time_time
    , fastestlap_rank
    , fastestlap_lap
    , fastestlap_time_time
    , fastestlap_averagespeed_units
    , fastestlap_averagespeed_speed
    , season
    , round
    , "date"
    , circuit_circuitid
    , load_dts
FROM {{ source('ergast', 'results') }}
