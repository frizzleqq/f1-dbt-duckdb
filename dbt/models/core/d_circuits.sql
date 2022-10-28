{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

WITH transformed AS (
    SELECT circuitId AS id
      , circuitName AS name
      , location_country AS country
      , location_locality AS locality
      , url AS url
      , location_lat AS latitude
      , location_long AS longitude
      , load_dts
    FROM {{ ref('ergast_circuits') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
