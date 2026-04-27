{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Historical products (SCD-style with prd_end_dt derived via LEAD upstream),
-- filtered to the current partition. stg_erp_PX_CAT_G1V2 is now
-- daily-partitioned (the seed is stamped onto the daily grid in the
-- landing model), so we co-filter on snapshot_date like every other join.
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
