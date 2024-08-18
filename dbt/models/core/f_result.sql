WITH results AS (
    SELECT *
    FROM {{ ref('ergast_results') }}
)

-- , constructor_results AS (
--     SELECT *
--     FROM {{ ref('ergast_constructor_results') }}
-- )

, constructor_standings AS (
    SELECT *
    FROM {{ ref('ergast_constructor_standings') }}
)

, driver_standings AS (
    SELECT *
    FROM {{ ref('ergast_driver_standings') }}
)

, qualifying AS (
    SELECT *
    FROM {{ ref('ergast_qualifying') }}
)

, result_status AS (
    SELECT *
    FROM {{ ref('ergast_status') }}
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
        , d_race.race_ref
        , d_circuit.circuit_ref
        , d_constructor.constructor_ref
        , d_driver.driver_ref
        , results.driver_number
        , results.grid AS starting_position
        , results.result_position
        , results.positiontext AS result_position_text
        , results.positionorder AS result_position_order
        , results.points AS result_points
        , result_status.status AS result_status
        , results.laps AS laps_completed
        , results.time AS finishing_time
        , results.milliseconds AS finishing_time_milliseconds
        , COALESCE(results.fastestlap_rank = 1, FALSE) AS has_fastest_lap
        , results.fastestlap AS fastest_lap_number
        , results.fastestlap_rank AS fastest_lap_position
        , results.fastestlaptime AS fastest_lap_time
        , results.fastestlapspeed AS fastest_lap_avg_speed
        , qualifying.qualifying_position
        , qualifying.q1 AS qualifying1_lap_time
        , qualifying.q2 AS qualifying2_lap_time
        , qualifying.q3 AS qualifying3_lap_time
        , driver_standings.points AS driver_season_points
        , driver_standings.driver_position AS driver_season_position
        , driver_standings.positiontext AS driver_season_position_text
        , driver_standings.wins AS driver_season_wins
        , constructor_standings.points AS constructor_season_points
        , constructor_standings.constructor_position AS constructor_season_position
        , constructor_standings.positiontext AS constructor_season_position_text
        , constructor_standings.wins AS constructor_season_wins
        -- TODO: constructor results?
    FROM results
    LEFT JOIN result_status ON result_status.statusid = results.statusid
    -- LEFT JOIN constructor_results
    --     ON
    --         constructor_results.raceid = results.raceid
    --         AND constructor_results.constructorid = results.constructorid
    LEFT JOIN constructor_standings
        ON
            constructor_standings.raceid = results.raceid
            AND constructor_standings.constructorid = results.constructorid
    LEFT JOIN driver_standings
        ON driver_standings.raceid = results.raceid AND driver_standings.driverid = results.driverid
    LEFT JOIN qualifying
        ON qualifying.raceid = results.raceid AND qualifying.driverid = results.driverid
    LEFT JOIN d_race
        ON d_race.race_id = results.raceid
    LEFT JOIN d_circuit
        ON d_circuit.circuit_ref = d_race.circuit_ref
    LEFT JOIN d_constructor
        ON d_constructor.constructor_id = results.constructorid
    LEFT JOIN d_driver
        ON d_driver.driver_id = results.driverid

)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'race_ref',
            'driver_ref',
        ])
    }} AS result_ref
    , *
FROM joined
