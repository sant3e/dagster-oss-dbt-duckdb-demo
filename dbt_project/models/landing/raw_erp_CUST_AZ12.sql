{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Static ERP customer reference — seeded once via `dbt seed`, then
-- stamped onto the daily partition grid here.
--
-- The seed itself (`{{ ref('CUST_AZ12') }}`) is unpartitioned reference
-- data (CID, BDATE, GEN — customer birth records that don't change day
-- to day). This landing model takes the entire seed and emits it once
-- per daily partition, tagging every row with the current partition
-- date so downstream `stg_erp_CUST_AZ12` + mart joins can uniformly
-- filter on snapshot_date.
--
-- Refresh cadence: every daily run re-reads the seed. If you edit the
-- seed CSV and re-run `dbt seed`, subsequent partitioned runs pick up
-- the new data automatically; older partitions retain their original
-- snapshot — because delete+insert only replaces the current partition,
-- history is preserved.
SELECT
    CID::VARCHAR AS CID,
    BDATE::DATE AS BDATE,
    GEN::VARCHAR AS GEN,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date
FROM {{ ref('CUST_AZ12') }}
