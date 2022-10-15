{{ config(materialized = 'view') }}

SELECT season
    , round
    , url
    , raceName
    , "date"
    , Circuit_circuitId
    , "time"
    , FirstPractice_date
    , SecondPractice_date
    , ThirdPractice_date
    , Qualifying_date
    , Sprint_date
    , FirstPractice_time
    , SecondPractice_time
    , ThirdPractice_time
    , Qualifying_time
    , Sprint_time
    , load_dts
FROM {{ source('ergast', 'races') }}
