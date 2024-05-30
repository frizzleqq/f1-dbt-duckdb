WITH results AS (
    SELECT
        *
    FROM {{ ref('ergast_results') }}
)

, qualifying AS (
    SELECT
        *
    FROM {{ ref('ergast_qualifying') }}
)

, constructor_results AS (
    SELECT
        *
    FROM {{ ref('ergast_constructor_results') }}
)

, constructor_standings AS (
    SELECT
        *
    FROM {{ ref('ergast_constructor_standings') }}
)

, driver_standings AS (
    SELECT
        *
    FROM {{ ref('ergast_driver_standings') }}
)

, d_circuit AS (
    SELECT
        *
    FROM {{ ref('d_circuit') }}
)

, d_constructor AS (
    SELECT
        *
    FROM {{ ref('d_constructor') }}
)

, d_driver AS (
    SELECT
        *
    FROM {{ ref('d_driver') }}
)

, d_race AS (
    SELECT
        *
    FROM {{ ref('d_race') }}
)


, joined AS (
    SELECT
        race_date
        , CONCAT(season, '-', round) AS race_id
        , circuit_circuitid AS circuit_id
        , driver_driverid AS driver_id
        , constructor_constructorid AS constructor_id
        , result_position
        , positiontext AS result_position_text
        , points AS result_points
        , result_status
        , grid AS starting_position
        , laps AS laps_completed
        , time_time AS result_time
        , time_millis AS result_milliseconds
        , COALESCE(fastestlap_rank = 1, FALSE) AS fastest_lap
        , fastestlap_rank AS fastest_lap_position
        , fastestlap_lap AS fastest_lap_number
        , fastestlap_time_time AS fastest_lap_time
        , fastestlap_averagespeed_speed AS fastest_lap_avg_speed
        , fastestlap_averagespeed_units AS fastest_lap_avg_speed_unit
        , constructor_constructorid AS constructor_id

        -- qualifying
        , qualifying.qualifying_position
        , qualifying.q1 AS qualifying1_lap_time
        , qualifying.q2 AS qualifying2_lap_time
        , qualifying.q3 AS qualifying3_lap_time
        , qualifying.qualifying_date + qualifying.qualifying_time AS qualifying_timestamp
        -- constructor results
        -- constructor standings
        -- driver standings
    FROM results
    LEFT JOIN qualifying
        ON results.raceid = qualifying.raceid AND results.driverid = qualifying.driverid
    LEFT JOIN constructor_results
        ON results.raceid = constructor_results.raceid AND results.constructorid = constructor_results.constructorid
    LEFT JOIN constructor_standings
        ON results.raceid = constructor_standings.raceid AND results.constructorid = constructor_standings.constructorid
    LEFT JOIN driver_standings
        ON results.raceid = driver_standings.raceid AND results.driverid = driver_standings.driverid
    LEFT JOIN d_circuit
        ON results.circuitid = d_circuit.circuit_id
    LEFT JOIN d_constructor
        ON results.constructorid = d_constructor.constructor_id
    LEFT JOIN d_driver
        ON results.driverid = d_driver.driver_id
    LEFT JOIN d_race
        ON results.raceid = d_race.race_id

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
