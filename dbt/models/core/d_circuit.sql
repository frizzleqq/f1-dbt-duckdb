SELECT
    circuitid AS circuit_id
    , circuitref AS circuit_key
    , circuit_name
    , location AS circuit_location
    , country AS circuit_country
    , url AS circuit_url
    , lat AS circuit_latitude
    , long AS circuit_longitude
FROM {{ ref('ergast_circuits') }}
