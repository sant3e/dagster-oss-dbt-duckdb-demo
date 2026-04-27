{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
        tags=['latest_available'],
    )
}}

-- Historical products (SCD-style), filtered to the current partition.
-- This is the first mart that bridges the monthly product cadence into
-- the daily pipeline — its direct upstream `stg_crm_prd_info` is tagged
-- `latest_available_source` (slow-cadence). Tagged `latest_available`
-- so `cross_partition_sensor` fires it daily in expansion mode, reusing
-- the latest monthly `stg_crm_prd_info` snapshot until a newer one
-- arrives. Downstream marts / reporting consume this daily view via
-- normal AutomationCondition.eager() — they don't need the tag.
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.prd_line AS product_line,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_start_dt AS start_date,
    pn.prd_end_dt AS end_date,
    CASE WHEN pn.prd_end_dt IS NULL THEN TRUE ELSE FALSE END AS is_current,
    pn.snapshot_date AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('stg_crm_prd_info') }} pn
LEFT JOIN {{ ref('stg_erp_PX_CAT_G1V2') }} pc
    ON pn.cat_id = pc.id
    AND pc.snapshot_date = pn.snapshot_date
WHERE pn.snapshot_date = '{{ var("snapshot_dt") }}'::DATE
