{{ config(materialized = 'view') }}

WITH source AS (
    SELECT *
    FROM {{ source('ergast', 'drivers') }}
)
SELECT *
FROM source
