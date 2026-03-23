{{ config(
    materialized='table'
) }}

WITH payments AS (
    SELECT 0 AS payment_id, 'Flex Fare trip' AS payment_type
    UNION ALL
    SELECT 1 AS payment_id, 'Credit card' AS payment_type
    UNION ALL
    SELECT 2 AS payment_id, 'Cash' AS payment_type
    UNION ALL
    SELECT 3 AS payment_id, 'No charge' AS payment_type
    UNION ALL
    SELECT 4 AS payment_id, 'Dispute' AS payment_type
    UNION ALL
    SELECT 5 AS payment_id, 'Unknown' AS payment_type
    UNION ALL
    SELECT 6 AS payment_id, 'Voided trip' AS payment_type
)
SELECT 
    payment_id,
    payment_type
FROM payments
