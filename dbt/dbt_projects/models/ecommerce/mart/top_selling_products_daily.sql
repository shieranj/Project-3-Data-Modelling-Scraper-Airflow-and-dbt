{{ config(
    materialized='incremental',
    partition_by= {
        'field': 'snapshot_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by= "product_id"
) }}

WITH product_sales AS (
    SELECT
        products.product_id,
        products.product_name,
        category.category_name,
        DATE(transactions.created_at) AS snapshot_date,
        SUM(transactions.total_amount) AS total_revenue,
        CURRENT_TIMESTAMP() AS dbt_processed_date
    
    FROM {{ ref('fct_transactions')}} AS transactions
    JOIN {{ ref('dim_products') }} AS products
        ON transactions.product_id = products.product_id
    JOIN {{ ref('dim_category') }} AS category
        ON products.category_id = category.category_id
    
    {% if is_incremental() %}
    WHERE DATE(transactions.created_at) > (
        SELECT MAX(snapshot_date)
        FROM {{ this }}
    )
    {% endif %}
    GROUP BY 
        products.product_id,
        products.product_name,
        category.category_name,
        snapshot_date
)

SELECT 
    product_id,
    product_name,
    category_name,
    snapshot_date,
    total_revenue,
    dbt_processed_date
FROM product_sales


