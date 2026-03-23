{{ config(
    materialized='table'
) }}

WITH trips AS (
    SELECT 1 AS trip_id, 'Street-hail' AS trip_type
    UNION ALL
    SELECT 2 AS trip_id, 'Dispatch' AS trip_type
)
SELECT 
    trip_id,
    trip_type
FROM trips
