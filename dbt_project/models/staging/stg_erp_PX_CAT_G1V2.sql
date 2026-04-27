{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Static product-category taxonomy. Reads the seed directly and stamps
-- the current partition's snapshot_date onto every row (same pattern as
-- stg_erp_CUST_AZ12).
SELECT
    ID::VARCHAR AS ID,
    TRIM(CAT) AS CAT,
    TRIM(SUBCAT) AS SUBCAT,
    TRIM(MAINTENANCE) AS MAINTENANCE,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('PX_CAT_G1V2') }}
