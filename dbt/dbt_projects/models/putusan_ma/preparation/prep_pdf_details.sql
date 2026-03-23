{{ config(
    materialized="incremental",
    unique_key = "nomor_putusan",
    partition_by = {
        "field" : "putusan_ym",
        "data_type": "date",
        "granularity": "month"
    }
)
 }}
SELECT
    SAFE_CAST(nomor_putusan AS STRING) AS nomor_putusan,
    SAFE_CAST(putusan_ym AS DATE) AS putusan_ym,
    {{ clean_string("nama_lengkap") }} AS nama_lengkap,
    {{ clean_string("tempat_lahir") }} AS tempat_lahir,
    {{ clean_string("umur_tanggal_lahir") }} AS umur_tanggal_lahir,
    {{ clean_string("jenis_kelamin") }} AS jenis_kelamin,
    {{ clean_string("kebangsaan") }} AS kebangsaan,
    {{ clean_string("agama") }} AS agama,
    {{ clean_string("pekerjaan") }} AS pekerjaan,
    {{ clean_string("tempat_tinggal") }} AS tempat_tinggal,
    {{ clean_string("keputusan") }} AS keputusan,
    SAFE_CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
    CURRENT_TIMESTAMP() AS dbt_processed_date
   
FROM {{ source('raw', 'pdf_details') }}
{% if is_incremental() %}
WHERE putusan_ym > (
    SELECT MAX(putusan_ym) FROM {{ this }}
)
{% endif %}
