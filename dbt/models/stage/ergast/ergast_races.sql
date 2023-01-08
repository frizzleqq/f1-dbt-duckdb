{{ config(materialized = 'view') }}

SELECT
    season
    , round
    , url
    , racename
    , "date"
    , circuit_circuitid
    , "time"
    , firstpractice_date
    , firstpractice_time
    , secondpractice_date
    , secondpractice_time
    , thirdpractice_date
    , thirdpractice_time
    , qualifying_date
    , qualifying_time
    , sprint_date
    , sprint_time
    , load_dts
FROM {{ source('ergast', 'races') }}
