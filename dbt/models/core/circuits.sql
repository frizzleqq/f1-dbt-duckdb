{{
    config(
        materialized = 'incremental',
        unique_key='circuit_id'
    )
}}

WITH circuits AS (
    SELECT circuitId AS circuit_id
      , circuitName AS name
      , Location AS location
      , url AS url
      , load_dts
    FROM {{ ref('ergast_circuits') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)
SELECT circuit_id
    , name
    , location
    , url
    , load_dts
FROM circuits