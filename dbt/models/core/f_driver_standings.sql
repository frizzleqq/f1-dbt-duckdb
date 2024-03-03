{{
    config(
        materialized = 'incremental',
        unique_key = 'driver_standing_id'
    )
}}

WITH transformed AS (
    SELECT
        CONCAT(season, '-', round) AS race_id
        , driver_driverid AS driver_id
        , constructors[1].constructorId AS constructor_id
        , driver_position
        , positiontext AS driver_position_text
        , points AS driver_points
        , wins AS driver_wins
        , load_dts
    FROM {{ ref('ergast_driver_standings') }}

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
                'driver_id',
            ])
        }} AS driver_standing_id
        , *
    FROM joined
)

SELECT *
FROM increment
