{{ config(materialized='external', format='parquet') }}

WITH results AS (
    SELECT *
    FROM {{ ref('ergast_results') }}
)

, qualifying AS (
    SELECT *
    FROM {{ ref('ergast_qualifying') }}
)

, constructor_results AS (
    SELECT *
    FROM {{ ref('ergast_constructor_results') }}
)

, constructor_standings AS (
    SELECT *
    FROM {{ ref('ergast_constructor_standings') }}
)

, driver_standings AS (
    SELECT *
    FROM {{ ref('ergast_driver_standings') }}
)

, d_circuit AS (
    SELECT *
    FROM {{ ref('d_circuit') }}
)

, d_constructor AS (
    SELECT *
    FROM {{ ref('d_constructor') }}
)

, d_driver AS (
    SELECT *
    FROM {{ ref('d_driver') }}
)

, d_race AS (
    SELECT *
    FROM {{ ref('d_race') }}
)


, joined AS (
    SELECT
        d_race.race_date
        , d_race.race_key
        , d_driver.driver_key
        -- , circuit_circuitid AS circuit_id
        -- , driver_driverid AS driver_id
        -- , constructor_constructorid AS constructor_id
        -- , result_position
        , results.positiontext AS result_position_text
        -- , points AS result_points
        -- , result_status
        -- , grid AS starting_position
        -- , laps AS laps_completed
        -- , time_time AS result_time
        -- , time_millis AS result_milliseconds
        -- , COALESCE(fastestlap_rank = 1, FALSE) AS fastest_lap
        -- , fastestlap_rank AS fastest_lap_position
        -- , fastestlap_lap AS fastest_lap_number
        -- , fastestlap_time_time AS fastest_lap_time
        -- , fastestlap_averagespeed_speed AS fastest_lap_avg_speed
        -- , fastestlap_averagespeed_units AS fastest_lap_avg_speed_unit
        -- , constructor_constructorid AS constructor_id

        -- TODO: qualifying
        , qualifying.qualifying_position
        , qualifying.q1 AS qualifying1_lap_time
        , qualifying.q2 AS qualifying2_lap_time
        , qualifying.q3 AS qualifying3_lap_time
        -- TODO: constructor results
        -- TODO: constructor standings
        -- TODO: driver standings
    FROM results
    LEFT JOIN qualifying
        ON qualifying.raceid = results.raceid AND qualifying.driverid = results.driverid
    LEFT JOIN constructor_results
        ON
            constructor_results.raceid = results.raceid
            AND constructor_results.constructorid = results.constructorid
    LEFT JOIN constructor_standings
        ON
            constructor_standings.raceid = results.raceid
            AND constructor_standings.constructorid = results.constructorid
    LEFT JOIN driver_standings
        ON driver_standings.raceid = results.raceid AND driver_standings.driverid = results.driverid
    LEFT JOIN d_race
        ON d_race.race_id = results.raceid
    LEFT JOIN d_circuit
        ON d_circuit.circuit_key = d_race.circuit_key
    LEFT JOIN d_constructor
        ON d_constructor.constructor_id = results.constructorid
    LEFT JOIN d_driver
        ON d_driver.driver_id = results.driverid

)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'race_key',
            'driver_key',
        ])
    }} AS result_key
    , *
FROM joined
