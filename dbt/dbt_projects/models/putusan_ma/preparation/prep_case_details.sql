{{ config(
    materialized="incremental",
    unique_key ="nomor_putusan",
    partition_by = {
        "field" : "tanggal_dibacakan",
        "data_type": "date",
        "granularity": "month"
    }
)
 }}
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY nomor_putusan ORDER BY ingestion_timestamp DESC) as rn
    FROM {{ source('raw', 'case_details') }}
    {% if is_incremental() %}
    WHERE tanggal_dibacakan >= (
        SELECT MAX(tanggal_dibacakan) FROM {{ this }}
    )
    {% endif %}
)
SELECT
    SAFE_CAST(nomor_putusan AS STRING) AS nomor_putusan,
    SAFE_CAST(tingkat_proses AS STRING) AS tingkat_proses,
    {{ clean_string("klasifikasi") }} AS klasifikasi,
    {{ clean_string("kata_kunci") }} AS kata_kunci,
    SAFE_CAST(tahun AS INTEGER) AS tahun,
    SAFE_CAST(tanggal_register AS DATE) AS tanggal_register,
    SAFE_CAST(lembaga_peradilan AS STRING) AS lembaga_peradilan,
    SAFE_CAST(jenis_lembaga_peradilan AS STRING) AS jenis_lembaga_peradilan,
    {{ clean_string("hakim_ketua") }} AS hakim_ketua,
    {{ clean_string("hakim_anggota") }} AS hakim_anggota,
    {{ clean_string("panitera") }} AS panitera,
    {{ clean_string("amar") }} AS amar,
    {{ clean_string("catatan_amar") }} AS catatan_amar,
    SAFE_CAST(tanggal_musyawarah AS DATE) AS tanggal_musyawarah,
    SAFE_CAST(tanggal_dibacakan AS DATE) AS tanggal_dibacakan,
    SAFE_CAST(pdf_link AS STRING) AS pdf_link,
    SAFE_CAST(scrape_timestamp AS TIMESTAMP) AS scrape_timestamp,
    SAFE_CAST(putusan_ym AS STRING) AS putusan_ym,
    SAFE_CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
    CURRENT_TIMESTAMP() AS dbt_processed_date,
FROM deduped
WHERE rn = 1
