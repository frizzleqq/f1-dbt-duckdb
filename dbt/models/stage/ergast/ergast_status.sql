{{ config(materialized = 'view') }}

SELECT
    statusid
    , status
FROM {{ source('ergast', 'status') }}
