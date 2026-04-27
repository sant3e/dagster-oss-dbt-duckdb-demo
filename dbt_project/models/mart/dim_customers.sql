{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Customer dimension for the current partition.
--
-- Event-style staging contract: stg_crm_cust_info emits ONLY rows that
-- changed on a given day (new customers + marital-status flips). So
-- filtering staging to `snapshot_date = current_partition` alone would
-- yield only today's events — for day-2/day-3 that is 8–13 rows and
-- downstream fact joins on `customer_key = customer_key` would lose
-- every historical customer.
--
-- To build the full customer universe as-of the current partition,
-- pick the LATEST row per `cst_id` across all staging partitions with
-- `snapshot_date <= current_partition`. Same carry-forward pattern for
-- stg_erp_LOC_A101 (event-style: day-2/day-3 contain only country-move
-- events; historical rows live on day-1). stg_erp_CUST_AZ12 is seed-fed
-- so every partition carries the full universe — filter to current is OK.
WITH latest_cust AS (
    SELECT *
    FROM (
        SELECT
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date, snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY snapshot_date DESC) AS rn
        FROM {{ ref('stg_crm_cust_info') }}
        WHERE snapshot_date <= '{{ var("snapshot_dt") }}'::DATE
    ) t
    WHERE rn = 1
),
latest_loc AS (
    SELECT *
    FROM (
        SELECT
            cid, cntry, snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY cid ORDER BY snapshot_date DESC) AS rn
        FROM {{ ref('stg_erp_LOC_A101') }}
        WHERE snapshot_date <= '{{ var("snapshot_dt") }}'::DATE
    ) t
    WHERE rn = 1
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    COALESCE(
        NULLIF(ci.cst_gndr, 'N/A'),
        NULLIF(ca.gen, 'N/A'),
        'N/A'
    ) AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM latest_cust ci
LEFT JOIN {{ ref('stg_erp_CUST_AZ12') }} ca
    ON ci.cst_key = ca.cid
    AND ca.snapshot_date = '{{ var("snapshot_dt") }}'::DATE
LEFT JOIN latest_loc la
    ON ci.cst_key = la.cid
