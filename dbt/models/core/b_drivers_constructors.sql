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
        , driver_driverid AS driver_id
        , constructor_constructorid AS constructor_id
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
