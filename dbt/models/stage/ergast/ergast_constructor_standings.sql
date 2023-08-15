{{ config(materialized = 'view') }}

SELECT
    CAST("position" AS INT) AS constructor_position
    , CAST(positiontext AS TEXT) AS positiontext
    , CAST(points AS DOUBLE) AS points
    , CAST(wins AS INT) AS wins
    , constructor_constructorid
    , constructor_url
    , constructor_name
    , constructor_nationality
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , load_dts
FROM {{ source('ergast', 'constructorStandings') }}
