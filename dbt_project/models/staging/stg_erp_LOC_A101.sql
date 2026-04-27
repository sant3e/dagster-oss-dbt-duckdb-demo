-- DuckDB-compatible: upper/lower/replace/trim supported.
-- INITCAP is not available in DuckDB 1.x — for the catch-all branch we
-- fall back to a simple trimmed value. For a demo this is fine; add a
-- title-case macro later if you care about cosmetics.
SELECT
    REPLACE(CID, '-', '') AS CID,
    CASE
        WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA', 'UNITED STATES OF AMERICA') THEN 'United States'
        WHEN UPPER(TRIM(CNTRY)) = 'UK' THEN 'United Kingdom'
        WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(CNTRY)) = 'FR' THEN 'France'
        WHEN CNTRY IS NULL OR UPPER(TRIM(CNTRY)) IN ('N/A', '', 'null') THEN 'N/A'
        ELSE TRIM(CNTRY)
    END AS CNTRY,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_erp_LOC_A101') }}
