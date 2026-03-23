{{ config(
    materialized='incremental',
    unique_key = 'product_id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    }
) }}

SELECT 
    product_id,
    product_name, 
    category_id,
    price,
    created_at, 
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('prep_products') }}

{% if is_incremental() %}
WHERE updated_at >= (
    SELECT MAX(updated_at) from {{ this }}
)
{% endif %}