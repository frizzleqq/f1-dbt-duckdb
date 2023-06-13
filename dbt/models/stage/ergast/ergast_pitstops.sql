{{ config(materialized = 'view') }}

SELECT
    driverid
    , CAST(lap AS INT) AS lap
    , CAST("stop" AS INT) AS pitstop_number
    , CAST("time" AS TIME) AS pitstop_time
    , CAST(duration AS DOUBLE) AS duration
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , CAST("date" AS DATE) AS race_date
    , racename
    , circuit_circuitid
    , load_dts
FROM {{ source('ergast', 'pitstops') }}
