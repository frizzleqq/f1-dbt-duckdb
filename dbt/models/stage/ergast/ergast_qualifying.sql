{{ config(materialized = 'view') }}

SELECT
    , qualifyId
    , raceid
    , driverid
    , constructorid
    , CAST(number AS INT) AS driver_number
    , CAST(position AS INT) AS qualifying_position
    , q1
    , q2
    , q3
FROM {{ source('ergast', 'qualifying') }}
