{{ config(materialized = 'view') }}

SELECT
    constructorstandingsid
    , raceid
    , constructorid
    , CAST(points AS DOUBLE) AS points
    , CAST(position AS INT) AS constructor_position
    , CAST(positiontext AS TEXT) AS positiontext
    , CAST(wins AS INT) AS wins
FROM {{ source('ergast', 'constructor_standings') }}
