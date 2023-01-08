{{ config(materialized = 'view') }}

SELECT
    driverid
    , "position"
    , "time"
    , season
    , round
    , "date"
    , laps_number
    , load_dts
FROM {{ source('ergast', 'laps') }}
