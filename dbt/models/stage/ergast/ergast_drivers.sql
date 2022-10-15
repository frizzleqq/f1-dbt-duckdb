{{ config(materialized = 'view') }}

SELECT driverId
    , url
    , givenName
    , familyName
    , dateOfBirth
    , nationality
    , permanentNumber
    , code
    , load_dts
FROM {{ source('ergast', 'drivers') }}
