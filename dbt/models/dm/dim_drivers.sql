{{ config(materialized = 'table') }}

WITH core AS (
    SELECT id
        , first_name
        , second_name
        , concat(first_name, ' ', second_name) as full_name
        , code
        , permanent_number
        , date_of_birth
        , nationality
        , url
        , load_dts
    FROM {{ ref('drivers') }}
)
SELECT *
FROM core
