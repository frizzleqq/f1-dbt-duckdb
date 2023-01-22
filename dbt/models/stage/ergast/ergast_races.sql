{{ config(materialized = 'view') }}

SELECT
    CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , url
    , racename
    , CAST("date" AS DATE) AS race_date
    , circuit_circuitid
    , "time" AS race_time
    , CAST(firstpractice_date AS DATE) AS firstpractice_date
    , firstpractice_time
    , CAST(secondpractice_date AS DATE) AS secondpractice_date
    , secondpractice_time
    , CAST(thirdpractice_date AS DATE) AS thirdpractice_date
    , thirdpractice_time
    , CAST(qualifying_date AS DATE) AS qualifying_date
    , qualifying_time
    , CAST(sprint_date AS DATE) AS sprint_date
    , sprint_time
    , load_dts
FROM {{ source('ergast', 'races') }}
