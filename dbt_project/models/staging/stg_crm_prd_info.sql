{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Product master — the upstream landing model (raw_crm_prd_info, tagged
-- `latest_available`) has already picked the latest monthly snapshot
-- on-or-before the daily partition and emitted snapshot_date = the daily
-- partition. This is a normal daily staging consumer: filter to the
-- current snapshot_date and derive SCD-style prd_end_dt via LEAD() within
-- the snapshot. AutomationCondition.eager() fires it automatically when
-- raw_crm_prd_info materializes for a partition.
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
