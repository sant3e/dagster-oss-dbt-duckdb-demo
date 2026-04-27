{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
        tags=['latest_available'],
    )
}}

-- Product master — MONTHLY source projected onto a daily snapshot.
-- This is the FIRST daily-partitioned model consuming the monthly
-- upstream (`raw/raw_prd_info_monthly`, which lives in DuckDB as
-- `raw.prd_info` and is populated by the Dagster landing asset when
-- the monthly CSV arrives).
--
-- Tagged `latest_available` because the upstream (the monthly raw
-- table) updates on a slower cadence than this model itself runs.
-- The `cross_partition_sensor` (ported from imp_finance_mart) detects
-- this tag, runs the model in expansion mode, and fires daily
-- partitions reusing the latest monthly snapshot until a newer one
-- arrives. The SQL below picks the correct monthly row at runtime via
-- `WHERE snapshot_month <= var('snapshot_dt')`.
--
-- Emits `snapshot_date = '{{ var("snapshot_dt") }}'::DATE` so downstream
-- staging / mart can uniformly filter all tables by the daily partition
-- key. The original `snapshot_month` is preserved for lineage.
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
