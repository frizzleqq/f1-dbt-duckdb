{{ config(materialized = 'view') }}

SELECT
    raceid
    , driverid
    , CAST(stop AS INT) AS pitstop_number
    , CAST(lap AS INT) AS lap
    , CAST(time AS TIME) AS pitstop_time
    , duration
    , CAST(milliseconds AS BIGINT) AS milliseconds
FROM {{ source('ergast', 'pit_stops') }}
