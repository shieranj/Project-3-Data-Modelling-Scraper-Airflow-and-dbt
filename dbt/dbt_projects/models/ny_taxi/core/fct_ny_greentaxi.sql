{{ config(
    materialized='incremental',
    partition_by= {
        'field': 'data_period',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by= ["vendor_id", "run_date", "payment_id"],
    incremental_strategy="insert_overwrite"
) }}

{% set data_period = var('data_period') %}

SELECT
    vendor_id,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_id,
    trip_id,
    congestion_surcharge,
    cbd_congestion_fee,
    data_period,
    run_date,
    CURRENT_TIMESTAMP() AS dbt_proccessed_time
FROM {{ ref('prep_ny_greentaxi') }} AS preparation

WHERE data_period = DATE('{{ data_period }}')