{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Cleaned ERP location master for the current partition. Reads directly
-- from the Dagster-landed raw table via the dbt source.
--
-- DuckDB-compatible: upper/lower/replace/trim supported. INITCAP is not
-- available in DuckDB 1.x — for the catch-all branch we fall back to a
-- simple trimmed value.
--
-- snapshot_date is cast to DATE in the SELECT because _landing_shared.py
-- writes the column as VARCHAR into DuckDB.
SELECT
    REPLACE(CID::VARCHAR, '-', '') AS CID,
    CASE
        WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES OF AMERICA') THEN 'United States'
        WHEN UPPER(TRIM(CNTRY)) = 'UK' THEN 'United Kingdom'
        WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(CNTRY)) = 'FR' THEN 'France'
        WHEN CNTRY IS NULL OR UPPER(TRIM(CNTRY)) IN ('N/A', '', 'null') THEN 'N/A'
        ELSE TRIM(CNTRY)
    END AS CNTRY,
    snapshot_date::DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ source('dagster_raw', 'loc_a101') }}
WHERE snapshot_date::DATE = '{{ var("snapshot_dt") }}'::DATE
