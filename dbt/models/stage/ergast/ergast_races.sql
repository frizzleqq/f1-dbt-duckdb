{{ config(materialized = 'view') }}

SELECT season
    , round
    , url
    , raceName
    , "date"
    , Circuit_circuitId
    , "time"
    , FirstPractice_date
    , FirstPractice_time
    , SecondPractice_date
    , SecondPractice_time
    , ThirdPractice_date
    , ThirdPractice_time
    , Qualifying_date
    , Qualifying_time
    , Sprint_date
    , Sprint_time
    , load_dts
FROM {{ source('ergast', 'races') }}
