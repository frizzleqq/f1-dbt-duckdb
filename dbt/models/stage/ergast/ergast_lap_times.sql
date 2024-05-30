{{ config(materialized = 'view') }}

SELECT
    raceid
    , driverid
    , CAST(lap AS INT) AS lap
    , CAST(position AS INT) AS race_position
    , time AS lap_time
    , CAST(milliseconds AS BIGINT) AS lap_milliseconds
FROM {{ source('ergast', 'lap_times') }}
