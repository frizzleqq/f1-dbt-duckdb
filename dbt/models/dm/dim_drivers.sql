{{ config(materialized = 'table') }}

SELECT driver_id
    , second_name
    , first_name
    , code
    , permanent_number
    , date_of_birth
    , nationality
    , url
    , load_dts
FROM {{ ref('drivers') }}
