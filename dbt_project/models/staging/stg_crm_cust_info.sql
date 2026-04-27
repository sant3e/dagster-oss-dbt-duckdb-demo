{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Cleaned customer master for the current partition. Ported from Snowflake:
-- QUALIFY rewritten as CTE + WHERE rn=1 (DuckDB has no QUALIFY).
--
-- The ROW_NUMBER dedupe protects against duplicate cst_id rows within the
-- same daily snapshot (if a source ever repeats a customer in a single CSV).
-- Cross-day dedup is not needed — each partition is processed in isolation.
WITH cleaned AS (
    SELECT
        cst_id,
        cst_key,
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
        cst_create_date,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM {{ ref('raw_crm_cust_info') }}
    WHERE snapshot_date = '{{ var("snapshot_dt") }}'::DATE
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
