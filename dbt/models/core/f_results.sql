{{
    config(
        materialized = 'incremental',
        unique_key = 'result_id'
    )
}}

WITH transformed AS (
    SELECT CAST("date" AS DATE) AS race_date
        , concat(season, '-', round) AS race_id
        , Circuit_circuitId AS circuit_id
        , Driver_driverId AS driver_id
        , Constructor_constructorId AS constructor_id
        , position AS result_position
        , positionText AS result_position_text
        , points AS result_points
        , status AS result_status
        , grid AS starting_position
        , laps AS laps_completed
        , Time_time AS result_time
        , Time_millis AS result_milliseconds
        , CASE WHEN FastestLap_rank = 1 THEN true ELSE false END AS fastest_lap
        , FastestLap_rank AS fastest_lap_position
        , FastestLap_lap AS fastest_lap_number
        , FastestLap_Time_time AS fastest_lap_time
        , FastestLap_AverageSpeed_speed AS fastest_lap_avg_speed
        , FastestLap_AverageSpeed_units AS fastest_lap_avg_speed_unit
        , load_dts
    FROM {{ ref('ergast_results') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

),

increment AS (
    SELECT {{
            dbt_utils.surrogate_key([
                'race_id',
                'driver_id',
            ])
        }} AS result_id
        , *
    FROM transformed
)

SELECT *
FROM increment