{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Cleaned customer master for the current partition. Reads directly from
-- the Dagster-landed raw table via the dbt source (which collapses onto
-- the Python landing AssetKey via meta.dagster.asset_key in sources.yml).
--
-- The ROW_NUMBER dedupe protects against duplicate cst_id rows within the
-- same daily snapshot (if a source ever repeats a customer in a single CSV).
-- Cross-day dedup is not needed — each partition is processed in isolation.
--
-- snapshot_date is cast to DATE in the SELECT because _landing_shared.py
-- writes the column as VARCHAR into DuckDB; casting here prevents VARCHAR
-- leaking into downstream mart joins.
WITH cleaned AS (
    SELECT
        cst_id::INTEGER AS cst_id,
        cst_key::VARCHAR AS cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date::DATE AS cst_create_date,
        snapshot_date::DATE AS snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM {{ source('dagster_raw', 'cust_info') }}
    WHERE snapshot_date::DATE = '{{ var("snapshot_dt") }}'::DATE
)
SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM cleaned
WHERE rn = 1
