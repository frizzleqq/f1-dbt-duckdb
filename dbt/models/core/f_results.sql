{{
    config(
        materialized = 'incremental',
        unique_key = 'result_id'
    )
}}

WITH transformed AS (
    SELECT
        race_date
        , CONCAT(season, '-', round) AS race_id
        , circuit_circuitid AS circuit_id
        , driver_driverid AS driver_id
        , constructor_constructorid AS constructor_id
        , result_position
        , positiontext AS result_position_text
        , points AS result_points
        , result_status
        , grid AS starting_position
        , laps AS laps_completed
        , time_time AS result_time
        , time_millis AS result_milliseconds
        , COALESCE(fastestlap_rank = 1, FALSE) AS fastest_lap
        , fastestlap_rank AS fastest_lap_position
        , fastestlap_lap AS fastest_lap_number
        , fastestlap_time_time AS fastest_lap_time
        , fastestlap_averagespeed_speed AS fastest_lap_avg_speed
        , fastestlap_averagespeed_units AS fastest_lap_avg_speed_unit
        , load_dts
    FROM {{ ref('ergast_results') }}

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
        }} AS result_id
        , *
    FROM transformed
)

SELECT *
FROM increment
