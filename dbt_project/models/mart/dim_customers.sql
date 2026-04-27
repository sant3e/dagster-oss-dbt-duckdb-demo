{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Customer dimension for the current partition. Merges CRM staging with
-- ERP reference (seeded, not partitioned) and location master (partitioned).
-- `customer_key` is a surrogate generated within the partition; downstream
-- fact joins always co-filter on snapshot_date so keys only need to be
-- consistent within a single partition.
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    COALESCE(
        NULLIF(ci.cst_gndr, 'N/A'),
        NULLIF(ca.gen, 'N/A'),
        'N/A'
    ) AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date,
    ci.snapshot_date AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('stg_crm_cust_info') }} ci
LEFT JOIN {{ ref('stg_erp_CUST_AZ12') }} ca
    ON ci.cst_key = ca.cid
LEFT JOIN {{ ref('stg_erp_LOC_A101') }} la
    ON ci.cst_key = la.cid
    AND la.snapshot_date = ci.snapshot_date
WHERE ci.snapshot_date = '{{ var("snapshot_dt") }}'::DATE
