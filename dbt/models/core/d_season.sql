{{ config(materialized='external', format='parquet') }}

SELECT
    season
    , url AS season_url
FROM {{ ref('ergast_seasons') }}
