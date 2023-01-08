{{
    config(
        materialized = 'incremental',
        unique_key = 'driver_id'
    )
}}

WITH drivers AS (
    SELECT *
    FROM {{ ref('ergast_drivers') }}

    {% if is_incremental() -%}
    WHERE load_dts > (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}
)

, results_aggregated AS (
    SELECT
        driver_id
        , MIN(race_date) AS first_race_date
        , MAX(race_date) AS most_recent_race_date
        , COUNT(DISTINCT race_id) AS number_of_races
        , SUM(result_points) AS lifetime_points
    FROM {{ ref('f_results') }}
    GROUP BY driver_id
)

, transformed AS (
    SELECT
        drivers.driverid AS driver_id
        , drivers.familyname AS driver_second_name
        , drivers.givenname AS driver_first_name
        , CONCAT(drivers.givenname, ' ', drivers.familyname) AS driver_full_name
        , drivers.code AS driver_code
        , drivers.permanentnumber AS driver_permanent_number
        , CAST(drivers.dateofbirth AS DATE) AS driver_date_of_birth
        , drivers.nationality AS driver_nationality
        , drivers.url AS driver_url
        , results_aggregated.first_race_date
        , results_aggregated.most_recent_race_date
        , COALESCE(results_aggregated.number_of_races, 0) AS number_of_races
        , COALESCE(results_aggregated.lifetime_points, 0) AS lifetime_points
        , drivers.load_dts
    FROM drivers
    LEFT JOIN results_aggregated ON results_aggregated.driver_id = drivers.driverid
)

SELECT *
FROM transformed
