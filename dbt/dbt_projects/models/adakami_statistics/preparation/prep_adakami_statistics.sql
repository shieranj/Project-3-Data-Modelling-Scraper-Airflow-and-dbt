{{ config(
    materialized="incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {
        "field":"data_period",
        "data_type": "date",
        "granularity":"day"
    }
) }}
SELECT
    SAFE_CAST(disburse_amount_total AS INTEGER) AS disburse_amount_total,
    SAFE_CAST(disburse_amount_this_year AS INTEGER) AS disburse_amount_this_year,
    SAFE_CAST(outstanding_loan_principal_total AS INTEGER) AS outstanding_loan_principal_total,
    SAFE_CAST(borrower_count_total AS INTEGER) AS borrower_count_total,
    SAFE_CAST(disburse_amount_last_month AS INTEGER) AS disburse_amount_last_month,
    SAFE_CAST(lender_count_total AS INTEGER) AS lender_count_total,
    SAFE_CAST(lender_count_year AS INTEGER) AS lender_count_year,
    SAFE_CAST(lender_count_month AS INTEGER) AS lender_count_month,
    SAFE_CAST(borrower_count_year AS INTEGER) AS borrower_count_year,
    SAFE_CAST(borrower_count_month AS INTEGER) AS borrower_count_month,
    SAFE_CAST(fetch_time AS TIMESTAMP) AS fetch_time,
    SAFE_CAST(data_period AS DATE) AS data_period,
    CURRENT_TIMESTAMP() dbt_processed_date
FROM {{ source('raw','adakami_daily_statistics') }}

{% if is_incremental() %}
WHERE data_period > (
    SELECT MAX(data_period) from {{ this }}
)
{% endif %}
