{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Ported from Snowflake: length(x::VARCHAR) replaces LENGTH(CAST(x AS VARCHAR)).
-- Now daily-partitioned — filters the upstream (raw_erp_CUST_AZ12) to the
-- current partition.
SELECT
    CASE
        WHEN length(CID::VARCHAR) = 13 THEN SUBSTRING(CID, 4)
        WHEN length(CID::VARCHAR) = 10 THEN CID
        ELSE NULL
    END AS CID,
    CASE
        WHEN BDATE > CURRENT_DATE THEN NULL
        ELSE BDATE
    END AS BDATE,
    CASE
        WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS GEN,
    snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('raw_erp_CUST_AZ12') }}
WHERE snapshot_date = '{{ var("snapshot_dt") }}'::DATE
