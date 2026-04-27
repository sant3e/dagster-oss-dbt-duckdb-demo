-- Ported from Snowflake:
--   LENGTH(CAST(x AS STRING))  → length(x::VARCHAR)
--   TO_DATE(CAST(x AS STRING), 'YYYYMMDD') → strptime(x::VARCHAR, '%Y%m%d')::DATE
WITH raw_data AS (
    SELECT
        sls_ord_num,
        REPLACE(sls_prd_key, '-', '_') AS sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt IS NULL OR sls_order_dt <= 0
              OR length(sls_order_dt::VARCHAR) != 8
            THEN NULL
            ELSE strptime(sls_order_dt::VARCHAR, '%Y%m%d')::DATE
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt IS NULL OR sls_ship_dt <= 0
              OR length(sls_ship_dt::VARCHAR) != 8
            THEN NULL
            ELSE strptime(sls_ship_dt::VARCHAR, '%Y%m%d')::DATE
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt IS NULL OR sls_due_dt <= 0
              OR length(sls_due_dt::VARCHAR) != 8
            THEN NULL
            ELSE strptime(sls_due_dt::VARCHAR, '%Y%m%d')::DATE
        END AS sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    FROM {{ ref('raw_crm_sales_details') }}
),
step1_corrected_price AS (
    SELECT *,
        CASE WHEN sls_price < 0 THEN ABS(sls_price) ELSE sls_price END AS corrected_price
    FROM raw_data
),
step2_corrected_sales AS (
    SELECT *,
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0
              OR sls_sales != (sls_quantity * corrected_price)
            THEN sls_quantity * corrected_price
            ELSE sls_sales
        END AS corrected_sales
    FROM step1_corrected_price
),
step3_final_price AS (
    SELECT *,
        CASE
            WHEN corrected_price IS NULL OR corrected_price = 0 THEN
                CASE
                    WHEN sls_quantity IS NOT NULL AND sls_quantity != 0 THEN
                        corrected_sales / sls_quantity
                    ELSE corrected_price
                END
            ELSE corrected_price
        END AS final_price
    FROM step2_corrected_sales
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    corrected_sales AS sls_sales,
    sls_quantity,
    final_price AS sls_price,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM step3_final_price
