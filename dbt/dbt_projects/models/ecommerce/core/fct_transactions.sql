{{ config(
    materialized='incremental',
    unique_key = 'transaction_id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by = 'product_id'
) }}

SELECT 
    transaction_id,
    customer_id, 
    product_id,
    unit_price,
    quantity,
    total_amount,
    created_at, 
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('prep_transactions') }}

{% if is_incremental() %}
WHERE updated_at >= (
    SELECT MAX(updated_at) from {{ this }}
)
{% endif %}