{{ config(
    materialized='incremental',
    partition_by= {
        'field': 'data_period',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by= "vendor_id",
    incremental_strategy="insert_overwrite"
) }}

{% set data_period = var('data_period') %}

SELECT
    fct_greentaxi.vendor_id,
    dim_vendors.vendor_name,
    AVG(fct_greentaxi.fare_amount) AS avg_fare,
    SUM(fct_greentaxi.trip_distance) AS total_distance,
    COUNT(*) AS trip_count,
    MIN(fct_greentaxi.tip_amount) AS min_tips,
    MAX(fct_greentaxi.tip_amount) AS max_tips,
    SUM(fct_greentaxi.tip_amount) AS total_tips_earned,
    SUM(fct_greentaxi.total_amount) AS total_revenue,
    fct_greentaxi.data_period AS data_period,
    CURRENT_TIMESTAMP() AS dbt_proccessed_time

FROM {{ ref('fct_ny_greentaxi') }} AS fct_greentaxi
JOIN {{ ref('dim_vendors') }} AS dim_vendors
    ON fct_greentaxi.vendor_id = dim_vendors.vendor_id
    
WHERE fct_greentaxi.data_period = DATE('{{ data_period }}')
GROUP BY 
    fct_greentaxi.vendor_id,
    dim_vendors.vendor_name,
    fct_greentaxi.data_period
