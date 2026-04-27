{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Now daily-partitioned — filters upstream (raw_erp_PX_CAT_G1V2) to the
-- current partition.
SELECT
    ID AS ID,
    TRIM(CAT) AS CAT,
    TRIM(SUBCAT) AS SUBCAT,
    TRIM(MAINTENANCE) AS MAINTENANCE,
    snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_erp_PX_CAT_G1V2') }}
WHERE snapshot_date = '{{ var("snapshot_dt") }}'::DATE
