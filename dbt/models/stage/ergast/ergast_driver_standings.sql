{{ config(materialized = 'view') }}

SELECT
    CAST("position" AS INT) AS driver_position
    , CAST(positiontext AS TEXT) AS positiontext
    , CAST(points AS DOUBLE) AS points
    , CAST(wins AS INT) AS wins
    , driver_driverid
    , CAST(driver_permanentnumber AS INT) AS driver_permanentnumber
    , driver_code
    , driver_url
    , driver_givenname
    , driver_familyname
    , CAST(driver_dateofbirth AS DATE) AS driver_dateofbirth
    , driver_nationality
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , constructors
    , load_dts
FROM {{ source('ergast', 'driverStandings') }}
