{{ config(materialized = 'view') }}

SELECT
    constructorid
    , constructorref
    , name AS constructor_name
    , nationality
    , url
FROM {{ source('ergast', 'constructors') }}
