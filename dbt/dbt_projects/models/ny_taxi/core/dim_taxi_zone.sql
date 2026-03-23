{{ config(
    materialized='table'
) }}

SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM {{ ref('prep_taxi_zone') }}
