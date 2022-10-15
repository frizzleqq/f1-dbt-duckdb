{{
    config(
        materialized = 'incremental',
        unique_key='id'
    )
}}

WITH stage AS (
    SELECT constructorId AS id
        , name
        , nationality
        , url
        , load_dts
    FROM {{ ref('ergast_constructors') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)
SELECT *
FROM stage
