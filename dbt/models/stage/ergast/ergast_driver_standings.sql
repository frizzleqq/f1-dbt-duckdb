{{ config(materialized = 'view') }}

SELECT
     driverStandingsId
    , raceId
    , driverId
    , CAST(points AS DOUBLE) AS points
    , CAST(position AS INT) AS driver_position
    , CAST(positiontext AS TEXT) AS positiontext
    , CAST(wins AS INT) AS wins
FROM {{ source('ergast', 'driver_standings') }}
