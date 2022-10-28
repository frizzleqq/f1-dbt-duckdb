{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

WITH transformed AS (
    SELECT CAST("date" AS DATE) AS race_date
        , concat(season, '-', round) AS race_id
        , driverId AS driver_id
        , Laps_number AS lap_number
        , "position" AS lap_position
        , "time" AS lap_time
        , load_dts
    FROM {{ ref('ergast_laps') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

),

increment AS (
    SELECT {{
            dbt_utils.surrogate_key([
                'race_id',
                'driver_id',
                'lap_number',
            ])
        }} AS id
        , *
    FROM transformed
)

SELECT *
FROM increment
