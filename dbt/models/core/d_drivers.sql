{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}

WITH transformed AS (
    SELECT driverId AS id
        , familyName AS second_name
        , givenName AS first_name
        , concat(givenName, ' ', familyName) as full_name
        , code AS code
        , permanentNumber AS permanent_number
        , CAST(dateOfBirth AS DATE) AS date_of_birth
        , nationality AS nationality
        , url
        , load_dts
    FROM {{ ref('ergast_drivers') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}

)

SELECT *
FROM transformed
