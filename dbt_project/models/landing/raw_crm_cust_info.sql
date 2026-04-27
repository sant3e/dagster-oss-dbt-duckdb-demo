-- Daily-snapshotted customer master from CRM.
-- Reads from a Dagster-populated table (raw.cust_info) that contains an
-- append-log of snapshots across partitions. We keep the latest snapshot
-- per cst_id so downstream staging sees a single current row per customer.

WITH ranked AS (
    SELECT
        cst_id::INTEGER AS cst_id,
        cst_key::VARCHAR AS cst_key,
        cst_firstname::VARCHAR AS cst_firstname,
        cst_lastname::VARCHAR AS cst_lastname,
        cst_marital_status::VARCHAR AS cst_marital_status,
        cst_gndr::VARCHAR AS cst_gndr,
        cst_create_date::DATE AS cst_create_date,
        snapshot_date::DATE AS snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY snapshot_date DESC, cst_create_date DESC
        ) AS rn
    FROM {{ source('dagster_raw', 'cust_info') }}
)
SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    snapshot_date
FROM ranked
WHERE rn = 1
