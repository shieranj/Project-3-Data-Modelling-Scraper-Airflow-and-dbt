{{ config(
    materialized='view'
) }}
SELECT
    SAFE_CAST(payment_id AS STRING) AS payment_id,
    SAFE_CAST(trx_id AS STRING) AS transaction_id,
    SAFE_CAST(amount AS FLOAT64) AS amount,
    SAFE_CAST(payment_method AS STRING) AS payment_method,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,
    SAFE_CAST(run_date AS DATE) AS run_date
FROM {{ source('raw','payments') }}


