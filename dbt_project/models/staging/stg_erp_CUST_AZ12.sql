{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Cleaned ERP customer reference. Reads the static seed directly and
-- stamps the current partition's snapshot_date onto every row.
--
-- The seed itself (`{{ ref('CUST_AZ12') }}`) is unpartitioned reference
-- data (CID, BDATE, GEN — birth records that don't change day-to-day).
-- This staging model stamps + cleans in one step, with `delete+insert`
-- on `snapshot_date` so each partition holds its own copy.
--
-- Refresh cadence: every daily run re-reads the seed. If you edit the
-- seed CSV and re-run `dbt seed`, subsequent partitioned runs pick up
-- the new data automatically; older partitions retain their original
-- snapshot.
--
-- Ported from Snowflake: length(x::VARCHAR) replaces LENGTH(CAST(x AS VARCHAR)).
SELECT
    CASE
        WHEN length(CID::VARCHAR) = 13 THEN SUBSTRING(CID, 4)
        WHEN length(CID::VARCHAR) = 10 THEN CID
        ELSE NULL
    END AS CID,
    CASE
        WHEN BDATE > CURRENT_DATE THEN NULL
        ELSE BDATE::DATE
    END AS BDATE,
    CASE
        WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'N/A'
    END AS GEN,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('CUST_AZ12') }}
