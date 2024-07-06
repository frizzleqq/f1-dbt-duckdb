{{ config(materialized = 'view') }}

SELECT
    driverid
    , driverref
    , CAST(number AS INT) AS driver_number
    , code
    , forename
    , surname
    , CAST(dob AS DATE) AS dob
    , nationality
    , url
FROM {{ source('ergast', 'drivers') }}
