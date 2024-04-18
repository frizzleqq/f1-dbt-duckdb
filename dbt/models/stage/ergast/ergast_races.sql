{{ config(materialized = 'view') }}

SELECT
    CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , url
    , racename
    , CAST(date AS DATE) AS race_date
    , circuit_circuitid
    , CAST(time AS TIME) AS race_time
    , CAST(firstpractice_date AS DATE) AS firstpractice_date
    , CAST(firstpractice_time AS TIME) AS firstpractice_time
    , CAST(secondpractice_date AS DATE) AS secondpractice_date
    , CAST(secondpractice_time AS TIME) AS secondpractice_time
    , CAST(thirdpractice_date AS DATE) AS thirdpractice_date
    , CAST(thirdpractice_time AS TIME) AS thirdpractice_time
    , CAST(qualifying_date AS DATE) AS qualifying_date
    , CAST(qualifying_time AS TIME) AS qualifying_time
    , CAST(sprint_date AS DATE) AS sprint_date
    , CAST(sprint_time AS TIME) AS sprint_time
    , load_dts
FROM {{ source('ergast', 'races') }}
