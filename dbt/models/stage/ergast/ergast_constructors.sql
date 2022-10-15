{{ config(materialized = 'view') }}

SELECT constructorId
    , url
    , name
    , nationality
    , load_dts
FROM {{ source('ergast', 'constructors') }}
