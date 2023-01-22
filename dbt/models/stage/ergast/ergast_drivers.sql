{{ config(materialized = 'view') }}

SELECT
    driverid
    , url
    , givenname
    , familyname
    , CAST(dateofbirth AS DATE) AS dateofbirth
    , nationality
    , CAST(permanentnumber AS INT) AS permanentnumber
    , code
    , load_dts
FROM {{ source('ergast', 'drivers') }}
