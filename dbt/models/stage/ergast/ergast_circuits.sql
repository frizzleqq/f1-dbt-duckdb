{{ config(materialized = 'view') }}

SELECT
    circuitid
    , url
    , circuitname
    , location_lat
    , location_long
    , location_locality
    , location_country
    , load_dts
FROM {{ source('ergast', 'circuits') }}
