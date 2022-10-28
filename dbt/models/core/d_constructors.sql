{{
    config(
        materialized = 'incremental',
        unique_key = 'constructor_id'
    )
}}

WITH transformed AS (
    SELECT constructorId AS constructor_id
        , name AS constructor_name
        , nationality AS constructor_nationality
        , url AS constructor_url
        , load_dts
    FROM {{ ref('ergast_constructors') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
