{{
    config(
        materialized = 'incremental',
        unique_key = 'pitstop_id'
    )
}}

WITH transformed AS (
    SELECT
        race_date
        , CONCAT(season, '-', round) AS race_id
        , circuit_circuitid AS circuit_id
        , driverid AS driver_id
        , lap AS lap_number
        , pitstop_number
        , race_date + pitstop_time AS pitstop_timestamp
        , duration AS pitstop_duration
        , load_dts
    FROM {{ ref('ergast_pitstops') }}

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
                'pitstop_number',
            ])
        }} AS pitstop_id
        , *
    FROM transformed
)

SELECT *
FROM increment
