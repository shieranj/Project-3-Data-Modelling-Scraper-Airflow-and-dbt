{{ config(
    materialized='table',
) }}

SELECT 
    DISTINCT category_id,
    category_name,
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('prep_products') }}