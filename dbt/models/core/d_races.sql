{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

WITH transformed AS (
    SELECT
        concat(season, '-', round) AS id
        , season
        , round
        , raceName AS name
        , url
        , "date" AS race_date
        , "time" AS race_time
        , Circuit_circuitId
        , FirstPractice_date AS free_practice_1_date
        , FirstPractice_time AS free_practice_1_time
        , SecondPractice_date AS free_practice_2_date
        , SecondPractice_time AS free_practice_2_time
        , ThirdPractice_date AS free_practice_3_date
        , ThirdPractice_time AS free_practice_3_time
        , Qualifying_date AS qualifying_date
        , Qualifying_time AS qualifying_time
        , Sprint_date AS sprint_date
        , Sprint_time AS sprint_time
        , load_dts
    FROM {{ ref('ergast_races') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
