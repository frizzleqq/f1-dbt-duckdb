WITH pitstops AS (
    SELECT
        *
    FROM {{ ref('ergast_pitstops') }}
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
        d_race.race_key
        , d_race.race_date
        , d_drivers.driver_key
        , pitstops.pitstop_number
        , pitstops.lap AS lap_number
        , d_race.race_date + pitstops.pitstop_time AS pitstop_timestamp
        , pitstops.duration AS pitstop_duration
        , CAST(pitstops.milliseconds AS DOUBLE) / 1000 AS pitstop_seconds
    FROM pitstops
    LEFT JOIN d_driver
        ON lap_times.driverid = d_driver.driver_id
    LEFT JOIN d_race
        ON lap_times.raceid = d_race.raceid
)

SELECT
    {{
        dbt_utils.generate_surrogate_key([
            'race_key',
            'driver_key',
            'pitstop_number',
        ])
    }} AS pitstop_key
    , *
FROM joined
