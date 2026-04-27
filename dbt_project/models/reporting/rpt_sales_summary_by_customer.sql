{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Sales summary by customer for the current partition — DuckDB date_diff()
-- replaces Snowflake DATEDIFF(). Aggregation happens within the partition
-- since fct_sales and dim_customers are both filtered to it.
SELECT
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.gender,
    c.marital_status,
    COUNT(f.order_number) AS total_orders,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity_sold,
    AVG(f.sales_amount) AS avg_order_value,
    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,
    date_diff('day', MIN(f.order_date), MAX(f.order_date)) AS days_since_first_order,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date
FROM {{ ref('fct_sales') }} f
JOIN {{ ref('dim_customers') }} c
    ON f.customer_key = c.customer_key
    AND c.snapshot_date = f.snapshot_date
WHERE f.snapshot_date = '{{ var("snapshot_dt") }}'::DATE
GROUP BY
    c.customer_key,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.gender,
    c.marital_status
ORDER BY total_sales DESC
