{{ config(materialized = 'view') }}

SELECT
    season
    , url
    , load_dts
FROM {{ source('ergast', 'seasons') }}
