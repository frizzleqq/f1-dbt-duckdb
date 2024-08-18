SELECT
    constructorid AS constructor_id
    , constructorref AS constructor_ref
    , constructor_name
    , nationality AS constructor_nationality
    , url AS constructor_url
FROM {{ ref('ergast_constructors') }}
