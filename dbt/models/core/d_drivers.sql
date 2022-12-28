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
),

driver_races AS (
    SELECT driver_id
        , MIN(race_date) AS first_race_date
        , MAX(race_date) AS most_recent_race_date
        , COUNT(DISTINCT race_id) AS number_of_races
        , SUM(result_points) AS lifetime_points
    FROM {{ ref('f_results') }}
    GROUP BY driver_id
),

transformed AS (
    SELECT drivers.driverId AS driver_id
        , familyName AS driver_second_name
        , givenName AS driver_first_name
        , concat(givenName, ' ', familyName) as driver_full_name
        , code AS driver_code
        , permanentNumber AS driver_permanent_number
        , CAST(dateOfBirth AS DATE) AS driver_date_of_birth
        , nationality AS driver_nationality
        , url AS driver_url
        , driver_races.first_race_date
        , driver_races.most_recent_race_date
        , COALESCE(driver_races.number_of_races, 0) AS number_of_races
        , COALESCE(driver_races.lifetime_points, 0) AS lifetime_points
        , load_dts
    FROM drivers
    LEFT JOIN driver_races ON driver_races.driver_id = drivers.driverId
)

SELECT *
FROM transformed
