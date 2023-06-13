{{ config(alias='f_results') }}

SELECT * FROM {{ ref('f_results') }}
