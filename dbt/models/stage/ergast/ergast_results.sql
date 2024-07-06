{{ config(materialized = 'view') }}

SELECT
    resultid
    , raceid
    , driverid
    , constructorid
    , CAST(number AS INT) AS driver_number
    , CAST(grid AS INT) AS grid
    , CAST(position AS INT) AS result_position
    , positiontext
    , CAST(positionorder AS INT) AS positionorder
    , CAST(points AS DOUBLE) AS points
    , CAST(laps AS INT) AS laps
    , time
    , CAST(milliseconds AS BIGINT) AS milliseconds
    , fastestlap
    , CAST(rank AS INT) AS fastestlap_rank
    , fastestlaptime
    , CAST(fastestlapspeed AS DOUBLE) AS fastestlapspeed
    , statusid
FROM {{ source('ergast', 'results') }}
