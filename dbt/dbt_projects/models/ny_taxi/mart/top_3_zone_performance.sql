{{ config(
    materialized='incremental',
    partition_by= {
        'field': 'data_period',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by= ["borough", "zone"],
    incremental_strategy="insert_overwrite"
) }}

{% set data_period = var('data_period') %}

WITH zone_metrics AS(
    SELECT
    fct_greentaxi.pickup_location_id,
    dim_tz.borough,
    dim_tz.zone,
    COUNT(*) AS trip_count,
    SUM(fct_greentaxi.total_amount) AS total_revenue,
    SUM(fct_greentaxi.trip_distance) AS total_distance,
    AVG(fct_greentaxi.fare_amount) AS avg_fare,
    SUM(fct_greentaxi.tip_amount) AS total_tips_earned,
    SAFE_DIVIDE(SUM(fct_greentaxi.total_amount), SUM(fct_greentaxi.trip_distance)) AS revenue_per_mile,
    fct_greentaxi.data_period AS data_period

    FROM {{ ref('fct_ny_greentaxi') }} AS fct_greentaxi
    JOIN {{ ref('dim_taxi_zone') }} AS dim_tz
        ON fct_greentaxi.pickup_location_id = dim_tz.location_id

    WHERE fct_greentaxi.data_period = DATE('{{ data_period }}')

    GROUP BY 
        fct_greentaxi.pickup_location_id,
        dim_tz.borough,
        dim_tz.zone,
        fct_greentaxi.data_period
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY data_period
            ORDER BY trip_count DESC
        ) AS zone_rank
    FROM zone_metrics
)

SELECT *,
    CURRENT_TIMESTAMP() AS dbt_proccessed_time
FROM ranked
WHERE zone_rank <= 3
