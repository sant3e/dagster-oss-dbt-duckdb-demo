{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Daily ERP customer location master, filtered to the current partition.
SELECT
    CID::VARCHAR AS CID,
    CNTRY::VARCHAR AS CNTRY,
    snapshot_date::DATE AS snapshot_date
FROM {{ source('dagster_raw', 'loc_a101') }}
WHERE snapshot_date::DATE = '{{ var("snapshot_dt") }}'::DATE
