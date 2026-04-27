{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
        tags=['latest_available_source'],
    )
}}

-- Product master staging — cleaned + SCD-shaped. Tagged
-- `latest_available_source` because this is the dbt-side "handle" on
-- the monthly product data. Its rowset only changes when the upstream
-- monthly source updates, even though it's stamped with a daily
-- snapshot_date.
--
-- Downstream marts that need to keep firing daily regardless of whether
-- the monthly source has updated (e.g. `dim_products_history`) carry
-- the `latest_available` tag and are driven by `cross_partition_sensor`
-- in expansion mode.
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
    snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_crm_prd_info') }}
WHERE snapshot_date = '{{ var("snapshot_dt") }}'::DATE
