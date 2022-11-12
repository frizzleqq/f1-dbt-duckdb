{{ config(materialized = 'view') }}

SELECT "number"
    , "position"
    , Driver_driverId
    , Constructor_constructorId
    , Circuit_circuitId
    , Q1
    , Q2
    , Q3
    , season
    , round
    , "date"
    , "time"
    , raceName
    , url
    , load_dts
FROM {{ source('ergast', 'qualifying') }}
