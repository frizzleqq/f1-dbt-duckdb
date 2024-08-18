SELECT
    circuitid AS circuit_id
    , circuitref AS circuit_ref
    , circuit_name
    , location AS circuit_location
    , country AS circuit_country
    , url AS circuit_url
    , lat AS circuit_latitude
    , lng AS circuit_longitude
FROM {{ ref('ergast_circuits') }}
