{{
    config(
        materialized = 'incremental',
        unique_key = 'lap_id'
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
    WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

),

joined AS (
    SELECT f.*
        , d.circuit_id
    FROM transformed AS f
    LEFT JOIN {{ ref('d_races') }} AS d ON d.race_id = f.race_id
),

increment AS (
    SELECT {{
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
