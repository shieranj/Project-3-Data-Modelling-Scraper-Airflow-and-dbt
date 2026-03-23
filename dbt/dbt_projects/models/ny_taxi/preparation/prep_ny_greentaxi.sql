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
    SAFE_CAST(vendor_id AS INTEGER) AS vendor_id,
    SAFE_CAST(lpep_pickup_datetime AS TIMESTAMP) AS lpep_pickup_datetime,
    SAFE_CAST(lpep_dropoff_datetime AS TIMESTAMP) AS lpep_dropoff_datetime,
    SAFE_CAST(ratecode_id AS INTEGER) AS ratecode_id,
    SAFE_CAST(pu_location_id AS INTEGER) AS pickup_location_id,
    SAFE_CAST(do_location_id AS INTEGER) AS dropoff_location_id,
    SAFE_CAST(passenger_count AS INTEGER) AS passenger_count,
    SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
    SAFE_CAST(fare_amount AS FLOAT64) AS fare_amount,
    SAFE_CAST(extra AS FLOAT64) AS extra,
    SAFE_CAST(mta_tax AS FLOAT64) AS mta_tax,
    SAFE_CAST(tip_amount AS FLOAT64) AS tip_amount,
    SAFE_CAST(tolls_amount AS FLOAT64) AS tolls_amount,
    SAFE_CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
    SAFE_CAST(total_amount AS FLOAT64) AS total_amount,
    SAFE_CAST(payment_type AS INTEGER) AS payment_id,
    SAFE_CAST(trip_type AS INTEGER) AS trip_id,
    SAFE_CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
    SAFE_CAST(cbd_congestion_fee AS FLOAT64) AS cbd_congestion_fee,
    SAFE_CAST(data_period AS DATE) AS data_period,
    SAFE_CAST(run_date AS DATE) AS run_date
FROM {{ source('raw', 'NY_green_taxi') }}

WHERE data_period = DATE('{{ data_period }}')

