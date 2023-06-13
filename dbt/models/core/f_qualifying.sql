{{
    config(
        materialized = 'incremental',
        unique_key = 'qualifying_id'
    )
}}

WITH transformed AS (
    SELECT
        qualifying_date
        , CONCAT(season, '-', round) AS race_id
        , circuit_circuitid AS circuit_id
        , driver_driverid AS driver_id
        , constructor_constructorid AS constructor_id
        , qualifying_position
        , q1 AS qualifying1_lap_time
        , q2 AS qualifying2_lap_time
        , q3 AS qualifying3_lap_time
        , qualifying_time
        , load_dts
    FROM {{ ref('ergast_qualifying') }}

    {% if is_incremental() -%}
        WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

, increment AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'race_id',
                'driver_id',
            ])
        }} AS qualifying_id
        , *
    FROM transformed
)

SELECT *
FROM increment
