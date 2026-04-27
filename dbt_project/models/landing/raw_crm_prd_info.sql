-- Current product master — sourced from the MONTHLY Dagster-landed table.
-- Keeps the latest snapshot_month per prd_id so downstream staging sees a
-- single current row per product. Since this is monthly data consumed by
-- daily runs, downstream SQL just treats whatever is in here as "the
-- current month's view"; the cross-partition sensor in elt_pipelines
-- decides when it's safe to run the daily ELT for a given date.
WITH ranked AS (
    SELECT
        prd_id::INTEGER AS prd_id,
        prd_key::VARCHAR AS prd_key,
        prd_nm::VARCHAR AS prd_nm,
        prd_cost::INTEGER AS prd_cost,
        prd_line::VARCHAR AS prd_line,
        prd_start_dt::TIMESTAMP AS prd_start_dt,
        prd_end_dt::TIMESTAMP AS prd_end_dt,
        snapshot_month::DATE AS snapshot_month,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY snapshot_month DESC
        ) AS rn
    FROM {{ source('dagster_raw', 'prd_info') }}
)
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    snapshot_month
FROM ranked
WHERE rn = 1
