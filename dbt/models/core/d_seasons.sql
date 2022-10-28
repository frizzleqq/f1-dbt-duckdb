{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

WITH transformed AS (
    SELECT season AS id
        , url
        , load_dts
    FROM {{ ref('ergast_seasons') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
