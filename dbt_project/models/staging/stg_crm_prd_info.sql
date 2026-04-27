-- LEAD()/INTERVAL supported in DuckDB; no changes needed from source.
SELECT
    prd_id AS prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    REPLACE(SUBSTRING(prd_key, 7), '-', '_') AS prd_key,
    prd_nm AS prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line,
    prd_start_dt::DATE AS prd_start_dt,
    (LEAD(prd_start_dt) OVER (
        PARTITION BY REPLACE(SUBSTRING(prd_key, 7), '-', '_')
        ORDER BY prd_start_dt
    ) - INTERVAL '1 day')::DATE AS prd_end_dt,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_crm_prd_info') }}
