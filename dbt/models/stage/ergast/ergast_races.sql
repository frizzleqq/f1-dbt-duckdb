{{ config(materialized = 'view') }}

SELECT
    raceid
    ,CAST(year AS INT) AS season
    , CAST(round AS INT) AS round
    , circuitid
    , name AS race_name
    , CAST(date AS DATE) AS race_date
    , CAST(time AS TIME) AS race_time
    , url
    , CAST(fp1_date AS DATE) AS fp1_date
    , CAST(fp1_time AS TIME) AS fp1_time
    , CAST(fp2_date AS DATE) AS fp2_date
    , CAST(fp2_time AS TIME) AS fp2_time
    , CAST(fp3_date AS DATE) AS fp3_date
    , CAST(fp3_time AS TIME) AS fp3_time
    , CAST(quali_date AS DATE) AS quali_date
    , CAST(quali_time AS TIME) AS quali_time
    , CAST(sprint_date AS DATE) AS sprint_date
    , CAST(sprint_time AS TIME) AS sprint_time
FROM {{ source('ergast', 'races') }}
