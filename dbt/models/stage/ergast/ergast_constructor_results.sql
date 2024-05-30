{{ config(materialized = 'view') }}

SELECT
    constructorResultsId
    ,raceId
    ,constructorId
    ,points AS constructor_points
    ,status
FROM {{ source('ergast', 'constructor_results') }}
