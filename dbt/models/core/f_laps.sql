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
        , circuit.circuitid AS circuit_id
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

, increment AS (
    SELECT
        {{
            dbt_utils.generate_surrogate_key([
                'race_id',
                'driver_id',
                'lap_number',
            ])
        }} AS lap_id
        , *
    FROM transformed
)

SELECT *
FROM increment
