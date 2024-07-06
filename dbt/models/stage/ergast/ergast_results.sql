{{ config(materialized = 'view') }}

SELECT
    resultId
    ,raceId
    ,driverId
    ,constructorId
    , CAST(number AS INT) AS driver_number
    , CAST(grid AS INT) AS grid
    , CAST(position AS INT) AS result_position
    ,positionText
    , CAST(positionOrder AS INT) AS positionorder
    , CAST(points AS DOUBLE) AS points
    , CAST(laps AS INT) AS laps
    ,time
    , CAST(milliseconds AS BIGINT) AS milliseconds
    ,fastestLap
    , CAST(rank AS INT) AS fastestlap_rank
    ,fastestLapTime
    ,CAST(fastestLapSpeed AS DOUBLE) AS fastestLapSpeed
    ,statusId
FROM {{ source('ergast', 'results') }}
