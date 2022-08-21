{{ config(materialized = 'view') }}

WITH source AS (
    SELECT *
    FROM {{ source('ergast', 'circuits') }}
)
SELECT *
FROM source
