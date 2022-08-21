{{
    config(
        materialized = 'incremental',
        unique_key='id'
    )
}}

WITH stage AS (
    SELECT circuitId AS id
      , circuitName AS name
      , Location AS location
      , url AS url
      , load_dts
    FROM {{ ref('ergast_circuits') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)
SELECT *
FROM stage
