{{ config(
    materialized='view'
) }}
SELECT
    SAFE_CAST(event_id AS STRING) AS event_id,
    SAFE_CAST(event_time AS TIMESTAMP) AS event_time,
    SAFE_CAST(payment_id AS STRING) AS payment_id,
    SAFE_CAST(trx_id AS STRING) AS transaction_id,
    SAFE_CAST(status AS STRING) AS status,
    SAFE_CAST(reason AS STRING) AS reason,
    SAFE_CAST(ingestion_time AS TIMESTAMP) AS ingestion_time
FROM {{ source('raw','payment_status') }}


