{{ config(materialized = 'view') }}

SELECT
    driverid
    , CAST("position" AS INT) AS lap_position
    , "time" AS lap_time
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , CAST("date" AS DATE) AS race_date
    , laps_number
    , JSON(REPLACE(circuit, '''', '"'))->>'$.circuitId' AS circuitId
    , load_dts
FROM {{ source('ergast', 'laps') }}
