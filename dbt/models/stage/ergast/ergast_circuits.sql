{{ config(materialized = 'view') }}

SELECT
    circuitid
    , circuitref
    , name AS circuit_name
    , location
    , country
    , CAST(lat AS DOUBLE) AS lat
    , CAST(lng AS DOUBLE) AS lng
    , url
FROM {{ source('ergast', 'circuits') }}
