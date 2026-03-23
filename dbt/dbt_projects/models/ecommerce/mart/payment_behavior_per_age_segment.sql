{{ config(
    materialized='table'
) }}

WITH customer_segment AS (
    SELECT
        customer_id,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) < 18 THEN 'Under 18'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 18 AND 24 THEN '18-24'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 25 AND 34 THEN '25-34'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 35 AND 44 THEN '35-44'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 45 AND 54 THEN '45-54'
            WHEN DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) BETWEEN 55 AND 64 THEN '55-64'
            ELSE 'Above 65'
        END AS age_group,

        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), join_date, DAY) < 30 THEN 'new'
            WHEN DATE_DIFF(CURRENT_DATE(), join_date, DAY) < 90 THEN 'recent'
            WHEN DATE_DIFF(CURRENT_DATE(), join_date, DAY) < 365 THEN 'active'
            ELSE 'loyal'
        END AS customer_status

    FROM {{ ref('dim_customers') }}
),
aggregated AS(
    SELECT
        customer_segment.age_group,
        customer_segment.customer_status,
        payments.payment_method,
        COUNT(*) AS total_payment_count,
        SUM(payments.amount) AS total_payment_values,
        CURRENT_TIMESTAMP() AS dbt_processed_date

    FROM {{ ref('fct_payments') }} AS payments
    JOIN {{ ref('fct_transactions' )}} AS transactions
        ON payments.transaction_id = transactions.transaction_id
    JOIN customer_segment
        ON transactions.customer_id = customer_segment.customer_id
    GROUP BY
        customer_segment.age_group,
        customer_segment.customer_status,
        payments.payment_method
)
SELECT * EXCEPT (rn)
FROM (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY age_group, customer_status
            ORDER BY total_payment_values DESC
        ) as rn
    FROM aggregated
)
WHERE rn = 1