{{
    config(
        materialized = 'incremental',
        unique_key = 'lap_id'
    )
}}

WITH transformed AS (
    SELECT
        race_date
        , CONCAT(season, '-', round) AS race_id
        , driverid AS driver_id
        , laps_number AS lap_number
        , lap_position
        , lap_time
        , load_dts
    FROM {{ ref('ergast_laps') }}

    {% if is_incremental() -%}
    WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

, races AS (
    SELECT * FROM {{ ref('d_races') }}
)

, joined AS (
    SELECT
        transformed.*
        , races.circuit_id
    FROM transformed
    LEFT JOIN races ON races.race_id = transformed.race_id
)

, increment AS (
    SELECT
        {{
            dbt_utils.surrogate_key([
                'race_id',
                'driver_id',
                'lap_number',
            ])
        }} AS lap_id
        , race_date
        , circuit_id
        , race_id
        , driver_id
        , lap_number
        , lap_position
        , lap_time
        , load_dts
    FROM joined
)

SELECT *
FROM increment
