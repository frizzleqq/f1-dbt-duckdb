{{
    config(
        materialized = 'incremental',
        unique_key = 'race_id'
    )
}}

WITH transformed AS (
    SELECT
        concat(season, '-', round) AS race_id
        , season AS race_season
        , round AS race_round
        , racename AS race_name
        , url AS race_url
        , race_date
        , race_date + race_time AS race_timestamp
        , circuit_circuitid AS circuit_id
        , firstpractice_date AS free_practice_1_date
        , firstpractice_date + firstpractice_time AS free_practice_1_timestamp
        , secondpractice_date AS free_practice_2_date
        , secondpractice_date + secondpractice_time AS free_practice_2_timestamp
        , thirdpractice_date AS free_practice_3_date
        , thirdpractice_date + thirdpractice_time AS free_practice_3_timestamp
        , qualifying_date AS qualifying_date
        , qualifying_date + qualifying_time AS qualifying_timestamp
        -- , sprint_date AS sprint_date
        -- , sprint_date + sprint_time AS sprint_timestamp
        , load_dts
    FROM {{ ref('ergast_races') }}

    {% if is_incremental() -%}
        WHERE load_dts > (SELECT max(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
