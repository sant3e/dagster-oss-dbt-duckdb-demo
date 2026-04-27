{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Daily-snapshotted CRM sales transactions, filtered to the current
-- partition. The Dagster landing asset has already deduped the raw table
-- on (ord_num, prd_key, snapshot_date) for this partition, so we can
-- pass the rows through unchanged.
SELECT
    sls_ord_num::VARCHAR AS sls_ord_num,
    sls_prd_key::VARCHAR AS sls_prd_key,
    sls_cust_id::INTEGER AS sls_cust_id,
    sls_order_dt::INTEGER AS sls_order_dt,
    sls_ship_dt::INTEGER AS sls_ship_dt,
    sls_due_dt::INTEGER AS sls_due_dt,
    sls_sales::INTEGER AS sls_sales,
    sls_quantity::INTEGER AS sls_quantity,
    sls_price::INTEGER AS sls_price,
    snapshot_date::DATE AS snapshot_date
FROM {{ source('dagster_raw', 'sales_details') }}
WHERE snapshot_date::DATE = '{{ var("snapshot_dt") }}'::DATE
