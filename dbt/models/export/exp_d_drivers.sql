{{ config(alias='d_drivers') }}

SELECT * FROM {{ ref('d_drivers') }}
