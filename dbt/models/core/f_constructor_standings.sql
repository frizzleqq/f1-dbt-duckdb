{{
    config(
        materialized = 'incremental',
        unique_key = 'constructor_standing_id'
    )
}}

WITH transformed AS (
    SELECT
        CONCAT(season, '-', round) AS race_id
        , constructor_constructorid AS constructor_id
        , constructor_position
        , positiontext AS constructor_position_text
        , points AS constructor_points
        , wins AS constructor_wins
        , load_dts
    FROM {{ ref('ergast_constructor_standings') }}

    {% if is_incremental() -%}
        WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

, races AS (
    SELECT
        race_id
        , race_date
    FROM {{ ref('d_races') }}
)

, joined AS (
    SELECT
        races.race_date
        , transformed.*
    FROM transformed
    LEFT JOIN races ON races.race_id = transformed.race_id
)

, increment AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'race_id',
                'constructor_id',
            ])
        }} AS constructor_standing_id
        , *
    FROM joined
)

SELECT *
FROM increment
