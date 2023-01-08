{{ config(materialized = 'view') }}

SELECT
    constructorid
    , url
    , name
    , nationality
    , load_dts
FROM {{ source('ergast', 'constructors') }}
