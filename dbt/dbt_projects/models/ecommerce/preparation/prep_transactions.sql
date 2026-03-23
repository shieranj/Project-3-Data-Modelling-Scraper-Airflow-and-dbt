{{ config(
    materialized='view'
) }}
SELECT
    SAFE_CAST(trx_id AS STRING) AS transaction_id,
    SAFE_CAST(customer_id AS STRING) AS customer_id,
    SAFE_CAST(product_id AS STRING) AS product_id,
    SAFE_CAST(unit_price AS FLOAT64) AS unit_price,
    SAFE_CAST(quantity AS INTEGER) AS quantity,
    SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,
    SAFE_CAST(run_date AS DATE) AS run_date
FROM {{ source('raw','transactions') }}


