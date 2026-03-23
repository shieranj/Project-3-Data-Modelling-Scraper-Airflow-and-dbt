{{ config(
    materialized='incremental',
    unique_key = 'customer_id',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by = 'customer_id'
) }}

SELECT 
    customer_id,
    customer_name, 
    email,
    birth_date,
    phone_number,
    address,
    join_date, 
    created_at, 
    updated_at,
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('prep_customers') }}

{% if is_incremental() %}
WHERE updated_at >= (
    SELECT MAX(updated_at) from {{ this }}
)
{% endif %}