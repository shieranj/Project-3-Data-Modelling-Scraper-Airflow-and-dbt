{{ config(
    materialized='table',
    partition_by = {
        'field': 'updated_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by = ["customer_id", "age_group", "customer_status"]
) }}

SELECT 
    customer_id,
    customer_name, 
    email, 
    birth_date,
    phone_number,
    join_date,
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
    END AS customer_status,
    updated_at, 
    CURRENT_TIMESTAMP() AS dbt_processed_date
FROM {{ ref ('dim_customers') }}


