-- Daily-snapshotted sales transactions from CRM.
-- Unlike customer master, sales are transactional — we union all snapshots
-- and dedupe on order number (keeping the latest snapshot's version).
WITH ranked AS (
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
        snapshot_date::DATE AS snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY sls_ord_num, sls_prd_key
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ source('dagster_raw', 'sales_details') }}
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,
    snapshot_date
FROM ranked
WHERE rn = 1
