{{ config(materialized='external', format='parquet') }}

WITH drivers AS (
    SELECT *
    FROM {{ ref('ergast_drivers') }}
)

-- , races AS (
--     SELECT *
--     FROM {{ ref('ergast_races') }}
-- )

-- , results AS (
--     SELECT *
--     FROM {{ ref('ergast_results') }}
-- )

-- , results_aggregated AS (
--     SELECT
--         results.driverid
--         , MIN(races.race_date) AS first_race_date
--         , MAX(races.race_date) AS most_recent_race_date
--         , COUNT(DISTINCT results.raceid) AS career_races
--         , SUM(results.points) AS career_points
--         , SUM(results.laps) AS career_laps
--     FROM results
--     INNER JOIN races ON races.driverid = results.driverid
--     GROUP BY driverid
-- )

SELECT
    drivers.driverid AS driver_id
    , drivers.driverref AS driver_ref
    , drivers.surname AS driver_second_name
    , drivers.forename AS driver_first_name
    , CONCAT(drivers.forename, ' ', drivers.surname) AS driver_full_name
    , drivers.code AS driver_code
    , drivers.driver_number
    , drivers.dob AS driver_date_of_birth
    , drivers.nationality AS driver_nationality
    , drivers.url AS driver_url
    -- , results_aggregated.first_race_date
    -- , results_aggregated.most_recent_race_date
    -- , COALESCE(results_aggregated.career_races, 0) AS career_races
    -- , COALESCE(results_aggregated.career_points, 0) AS career_points
    -- , COALESCE(results_aggregated.career_laps, 0) AS career_laps
FROM drivers
-- LEFT JOIN results_aggregated ON results_aggregated.driverid = drivers.driverid
