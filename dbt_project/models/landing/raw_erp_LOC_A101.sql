-- Daily-snapshotted customer location master from ERP.
-- Keeps latest snapshot per customer.
WITH ranked AS (
    SELECT
        CID::VARCHAR AS CID,
        CNTRY::VARCHAR AS CNTRY,
        snapshot_date::DATE AS snapshot_date,
        ROW_NUMBER() OVER (
            PARTITION BY CID
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM {{ source('dagster_raw', 'loc_a101') }}
)
SELECT
    CID,
    CNTRY,
    snapshot_date
FROM ranked
WHERE rn = 1
