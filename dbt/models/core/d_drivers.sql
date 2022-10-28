{{
    config(
        materialized = 'incremental',
        unique_key = 'driver_id'
    )
}}

WITH transformed AS (
    SELECT driverId AS driver_id
        , familyName AS driver_second_name
        , givenName AS driver_first_name
        , concat(givenName, ' ', familyName) as driver_full_name
        , code AS driver_code
        , permanentNumber AS driver_permanent_number
        , CAST(dateOfBirth AS DATE) AS driver_date_of_birth
        , nationality AS driver_nationality
        , url AS driver_url
        , load_dts
    FROM {{ ref('ergast_drivers') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
