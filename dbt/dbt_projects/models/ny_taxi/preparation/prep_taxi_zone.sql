{{ config(
    materialized='view',
) }}

SELECT 
    SAFE_CAST(LocationID AS INTEGER) AS location_id,
    SAFE_CAST(Borough AS STRING) AS borough,
    SAFE_CAST(Zone AS STRING) AS zone,
    SAFE_CAST(service_zone AS STRING) AS service_zone
FROM {{ source('raw', 'taxi_zone_lookup') }}


