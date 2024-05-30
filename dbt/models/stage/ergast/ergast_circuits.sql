{{ config(materialized = 'view') }}

SELECT
    circuitid
    , circuitref
    , name AS circuit_name
    , location
    , country
    , CAST(lat AS DOUBLE) AS lat
    , CAST(long AS DOUBLE) AS long
    , url
    , load_dts
FROM {{ source('ergast', 'circuits') }}
