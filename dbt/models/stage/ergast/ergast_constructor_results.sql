{{ config(materialized = 'view') }}

SELECT
    constructorresultsid
    , raceid
    , constructorid
    , points AS constructor_points
    , status
FROM {{ source('ergast', 'constructor_results') }}
