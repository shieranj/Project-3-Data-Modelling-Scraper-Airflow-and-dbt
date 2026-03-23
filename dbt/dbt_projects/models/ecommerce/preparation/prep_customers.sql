{{ config(
    materialized='view'
) }}
SELECT
    SAFE_CAST(customer_id AS STRING) AS customer_id,
    SAFE_CAST(name AS STRING) AS customer_name,
    SAFE_CAST(email AS STRING) AS email,
    SAFE_CAST(birthday AS DATE) AS birth_date,
    {{ standardize_phone('phone_number') }} AS phone_number,
    SAFE_CAST(address AS STRING) AS address,
    SAFE_CAST(join_date AS DATE) AS join_date,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at,
    SAFE_CAST(run_date AS DATE) AS run_date
FROM {{ source('raw','customers') }}


