{{ config(
    materialized='view'
) }}
SELECT
    SAFE_CAST(product_id AS STRING) AS product_id,
    SAFE_CAST(product_name AS STRING) AS product_name,
    SAFE_CAST(category_id AS STRING) AS category_id,
    SAFE_CAST(category_name AS STRING) AS category_name,
    SAFE_CAST(price AS FLOAT64) AS price,
    SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
    SAFE_CAST(updated_at AS TIMESTAMP) AS updated_at
FROM {{ source('raw','products') }}


