{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Static product-category taxonomy — seeded once via `dbt seed`, then
-- stamped onto the daily partition grid here (same pattern as
-- raw_erp_CUST_AZ12). See that file's header for the full rationale.
SELECT
    ID::VARCHAR AS ID,
    CAT::VARCHAR AS CAT,
    SUBCAT::VARCHAR AS SUBCAT,
    MAINTENANCE::VARCHAR AS MAINTENANCE,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date
FROM {{ ref('PX_CAT_G1V2') }}
