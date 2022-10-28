{{
    config(
        materialized = 'incremental',
        unique_key = 'season_id'
    )
}}

WITH transformed AS (
    SELECT season AS season_id
        , url AS season_url
        , load_dts
    FROM {{ ref('ergast_seasons') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
