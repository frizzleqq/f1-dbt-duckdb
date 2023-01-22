{{ config(materialized = 'view') }}

SELECT
    CAST(season AS INT) AS season
    , url
    , load_dts
FROM {{ source('ergast', 'seasons') }}
