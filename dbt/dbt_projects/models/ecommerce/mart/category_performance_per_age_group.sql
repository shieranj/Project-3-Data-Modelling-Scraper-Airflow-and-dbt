{{ config(
    materialized='table'
) }}

WITH customer_segment AS (
    SELECT
        customer_id,

        -- Age bucket
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) < 18 THEN 'Under 18'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 18 AND 24 THEN '18-24'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 25 AND 34 THEN '25-34'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 35 AND 44 THEN '35-44'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 45 AND 54 THEN '45-54'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 55 AND 64 THEN '55-64'
            ELSE 'Above 65'
        END AS age_group

    FROM {{ ref('dim_customers') }}
)
SELECT
    customer_segment.age_group,
    category.category_name,
    SUM(transactions.total_amount) AS total_revenue,
    CURRENT_TIMESTAMP() AS dbt_processed_date

FROM {{ ref('fct_transactions') }} AS transactions
JOIN {{ ref('dim_products') }} AS products
    ON transactions.product_id = products.product_id
JOIN {{ ref('dim_category') }} AS category
    ON products.category_id = category.category_id
JOIN customer_segment
    ON transactions.customer_id = customer_segment.customer_id
GROUP BY
    customer_segment.age_group,
    category.category_name
ORDER BY total_revenue DESC
