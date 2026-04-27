{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Product master — MONTHLY source projected onto a daily snapshot_date.
-- This model's DATA only changes when the monthly `raw_prd_info_monthly`
-- upstream updates (once a month). It's not part of the daily expansion
-- cascade — it fires only when the monthly upstream materializes (via
-- AutomationCondition.eager() cascading from the Dagster-owned monthly
-- asset). Downstream `stg_crm_prd_info` carries the slow-cadence shape
-- forward and is where the `latest_available_source` tag lives.
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
