{{ config(materialized = 'view') }}

SELECT
    driverid
    , CAST(lap AS INT) AS lap
    , CAST("stop" AS INT) AS pitstop_number
    , CAST("time" AS TIME) AS pitstop_time
    -- due to red flags the stop can take minutes
    , CASE
        WHEN REGEXP_MATCHES(CAST(duration AS TEXT), '\d+:.*')
            THEN (
                CAST(REGEXP_EXTRACT(CAST(duration AS TEXT), '(\d+):.*', 1) AS DOUBLE) * 60
                + CAST(REGEXP_EXTRACT(CAST(duration AS TEXT), '\d+:(.*)', 1) AS DOUBLE)
            )
        ELSE CAST(duration AS DOUBLE)
    END AS duration
    , CAST(season AS INT) AS season
    , CAST(round AS INT) AS round
    , CAST("date" AS DATE) AS race_date
    , racename
    , circuit_circuitid
    , load_dts
FROM {{ source('ergast', 'pitstops') }}
