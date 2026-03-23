{{ config(
    materialized='table'
) }}

WITH vendors AS (
    SELECT 1 AS vendor_id, 'Creative Mobile Technologies, LLC' AS vendor_name
    UNION ALL
    SELECT 2 AS vendor_id, 'Curb Mobility, LLC' AS vendor_name
    UNION ALL
    SELECT 3 AS vendor_id, 'Myle Technologies Inc' AS vendor_name
)
SELECT 
    vendor_id,
    vendor_name
FROM vendors
