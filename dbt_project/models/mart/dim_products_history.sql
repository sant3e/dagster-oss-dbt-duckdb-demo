-- Historical products (all SCD rows). No Snowflake-specific SQL.
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
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('stg_crm_prd_info') }} pn
LEFT JOIN {{ ref('stg_erp_PX_CAT_G1V2') }} pc
    ON pn.cat_id = pc.id
