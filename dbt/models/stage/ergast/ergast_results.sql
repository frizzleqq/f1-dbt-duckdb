{{ config(materialized = 'view') }}

SELECT "position"
	, positionText
	, points
	, grid
	, laps
	, status
	, Driver_driverId
	, Constructor_constructorId
	, Time_millis
	, Time_time
	, FastestLap_rank
	, FastestLap_lap
	, FastestLap_Time_time
	, FastestLap_AverageSpeed_units
	, FastestLap_AverageSpeed_speed
	, season
	, round
	, "date"
	, Circuit_circuitId
    , load_dts
FROM {{ source('ergast', 'results') }}
