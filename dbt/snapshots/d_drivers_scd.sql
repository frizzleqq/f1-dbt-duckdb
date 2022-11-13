{% snapshot d_drivers_scd %}

{{
    config(
        strategy = 'check',
        unique_key = 'driver_id',
        check_cols = ['driver_full_name', 'driver_code', 'driver_permanent_number'],
        updated_at = 'updated_at',
    )
}}

WITH drivers AS (
    SELECT *
    FROM {{ ref('ergast_drivers') }}

    {% if is_incremental() -%}
    WHERE load_dts >= (SELECT MAX(load_dts) FROM {{ this }})
    {%- endif %}
),

transformed AS (
    SELECT drivers.driverId AS driver_id
        , familyName AS driver_second_name
        , givenName AS driver_first_name
        , concat(givenName, ' ', familyName) as driver_full_name
        , code AS driver_code
        , permanentNumber AS driver_permanent_number
        , CAST(dateOfBirth AS DATE) AS driver_date_of_birth
        , nationality AS driver_nationality
        , url AS driver_url
        , load_dts
        , CAST(load_dts AS DATE) AS updated_at
    FROM drivers
)

SELECT *
FROM transformed

{% endsnapshot %}
