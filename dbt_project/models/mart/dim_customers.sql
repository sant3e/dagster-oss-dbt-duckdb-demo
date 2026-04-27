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
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('stg_crm_cust_info') }} ci
LEFT JOIN {{ ref('stg_erp_CUST_AZ12') }} ca
    ON ci.cst_key = ca.cid
LEFT JOIN {{ ref('stg_erp_LOC_A101') }} la
    ON ci.cst_key = la.cid
