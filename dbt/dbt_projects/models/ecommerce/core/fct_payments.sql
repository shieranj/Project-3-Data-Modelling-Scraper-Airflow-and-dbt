{{ config(
    materialized='incremental',
    unique_key = 'payment_id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by = 'payment_method'
) }}

SELECT 
    payment_id,
    transaction_id, 
    amount,
    LOWER(payment_method) AS payment_method,
    created_at, 
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('prep_payments') }}

{% if is_incremental() %}
WHERE updated_at >= (
    SELECT MAX(updated_at) from {{ this }}
)
{% endif %}