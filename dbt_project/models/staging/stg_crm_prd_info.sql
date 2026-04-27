{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
        tags=['latest_available_source'],
    )
}}

-- Product master staging — combines monthly→daily projection + SCD-style
-- cleaning in a single step. Reads directly from the Dagster-landed
-- monthly source via the dbt source (which collapses onto the Python
-- landing AssetKey ["raw","raw_prd_info_monthly"] via meta.dagster.asset_key).
--
-- Tagged `latest_available_source` because this is the dbt-side handle on
-- the slow-cadence monthly data. Its row-set only changes when the
-- upstream monthly source updates, even though it's stamped with a daily
-- snapshot_date.
--
-- Downstream marts that need to keep firing daily regardless of whether
-- the monthly source has updated (e.g. `dim_products_history`) carry the
-- `latest_available` tag and are driven by `cross_partition_sensor` in
-- expansion mode.
--
-- snapshot_date is cast to DATE in the SELECT because _landing_shared.py
-- writes the column as VARCHAR into DuckDB.
WITH latest_month AS (
    SELECT MAX(snapshot_month::DATE) AS snapshot_month
    FROM {{ source('dagster_raw', 'prd_info') }}
    WHERE snapshot_month::DATE <= '{{ var("snapshot_dt") }}'::DATE
),
projected AS (
    SELECT
        src.prd_id::INTEGER AS prd_id,
        src.prd_key::VARCHAR AS prd_key,
        src.prd_nm::VARCHAR AS prd_nm,
        src.prd_cost::INTEGER AS prd_cost,
        src.prd_line::VARCHAR AS prd_line,
        src.prd_start_dt::TIMESTAMP AS prd_start_dt,
        '{{ var("snapshot_dt") }}'::DATE AS snapshot_date
    FROM {{ source('dagster_raw', 'prd_info') }} src
    INNER JOIN latest_month lm
        ON src.snapshot_month::DATE = lm.snapshot_month
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    REPLACE(SUBSTRING(prd_key, 7), '-', '_') AS prd_key,
    prd_nm,
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
FROM projected
