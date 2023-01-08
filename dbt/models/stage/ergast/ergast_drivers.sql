{{ config(materialized = 'view') }}

SELECT
    driverid
    , url
    , givenname
    , familyname
    , dateofbirth
    , nationality
    , permanentnumber
    , code
    , load_dts
FROM {{ source('ergast', 'drivers') }}
