-- Current products only — ported from Snowflake:
--   QUALIFY ROW_NUMBER()... → CTE + WHERE rn = 1
WITH current_only AS (
    SELECT *
    FROM {{ ref('dim_products_history') }}
    WHERE is_current = TRUE
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_number ORDER BY start_date DESC) AS rn
    FROM current_only
)
SELECT * EXCLUDE (rn)
FROM ranked
WHERE rn = 1
