{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Daily CRM customer master, filtered to the current partition.
SELECT
    cst_id::INTEGER AS cst_id,
    cst_key::VARCHAR AS cst_key,
    cst_firstname::VARCHAR AS cst_firstname,
    cst_lastname::VARCHAR AS cst_lastname,
    cst_marital_status::VARCHAR AS cst_marital_status,
    cst_gndr::VARCHAR AS cst_gndr,
    cst_create_date::DATE AS cst_create_date,
    snapshot_date::DATE AS snapshot_date
FROM {{ source('dagster_raw', 'cust_info') }}
WHERE snapshot_date::DATE = '{{ var("snapshot_dt") }}'::DATE
