{{ config(materialized = 'view') }}

SELECT driverId
    , "position"
    , "time"
    , season
    , round
    , "date"
    , Laps_number
    , load_dts
FROM {{ source('ergast', 'laps') }}
