{{
    config(
        materialized = 'incremental',
        unique_key='driver_id'
    )
}}

WITH drivers AS (
    SELECT driverId AS driver_id
        , familyName AS second_name
        , givenName AS first_name
        , code AS code
        , permanentNumber AS permanent_number
        , CAST(dateOfBirth AS DATE) AS date_of_birth
        , nationality AS nationality
        , url AS url
        , load_dts
    FROM {{ ref('ergast_drivers') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)
SELECT driver_id
    , second_name
    , first_name
    , code
    , permanent_number
    , date_of_birth
    , nationality
    , url
    , load_dts
FROM drivers