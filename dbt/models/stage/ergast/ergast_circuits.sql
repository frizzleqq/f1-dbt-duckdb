{{ config(materialized = 'view') }}

SELECT
    circuitid
    , url
    , circuitname
    , CAST(location_lat AS DOUBLE) AS location_lat
    , CAST(location_long AS DOUBLE) AS location_long
    , location_locality
    , location_country
    , load_dts
FROM {{ source('ergast', 'circuits') }}
