{{
    config(
        materialized = 'incremental',
        unique_key = 'circuit_id'
    )
}}

WITH transformed AS (
    SELECT circuitId AS circuit_id
      , circuitName AS circuit_name
      , location_country AS circuit_country
      , location_locality AS circuit_locality
      , url AS circuit_url
      , location_lat AS circuit_latitude
      , location_long AS circuit_longitude
      , load_dts
    FROM {{ ref('ergast_circuits') }}

    {% if is_incremental() -%}
    WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
