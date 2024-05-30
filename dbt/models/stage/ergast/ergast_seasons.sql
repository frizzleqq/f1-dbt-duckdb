{{ config(materialized = 'view') }}

SELECT
    CAST(year AS INT) AS season
    , url
FROM {{ source('ergast', 'seasons') }}
