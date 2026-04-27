{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Product master — MONTHLY source projected onto a daily snapshot.
-- For the current daily partition, pick the most recent monthly snapshot
-- whose snapshot_month is on-or-before the partition date. This is the
-- standard latest-available-on-or-before pattern used to join a slow-moving
-- reference into a daily-running pipeline.
--
-- Emits `snapshot_date = '{{ var("snapshot_dt") }}'::DATE` so downstream
-- staging/mart can uniformly filter all tables by the daily partition key.
-- The original `snapshot_month` is preserved for lineage.
WITH latest_month AS (
    SELECT MAX(snapshot_month::DATE) AS snapshot_month
    FROM {{ source('dagster_raw', 'prd_info') }}
    WHERE snapshot_month::DATE <= '{{ var("snapshot_dt") }}'::DATE
)
SELECT
    src.prd_id::INTEGER AS prd_id,
    src.prd_key::VARCHAR AS prd_key,
    src.prd_nm::VARCHAR AS prd_nm,
    src.prd_cost::INTEGER AS prd_cost,
    src.prd_line::VARCHAR AS prd_line,
    src.prd_start_dt::TIMESTAMP AS prd_start_dt,
    src.prd_end_dt::TIMESTAMP AS prd_end_dt,
    src.snapshot_month::DATE AS snapshot_month,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date
FROM {{ source('dagster_raw', 'prd_info') }} src
INNER JOIN latest_month lm
    ON src.snapshot_month::DATE = lm.snapshot_month
