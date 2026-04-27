-- Customer RFM features built on the ELT reporting layer.
-- Owned by ml_team (see +group in dbt_project.yml). This model
-- is consumed by ml_pipelines' segmentation.py and churn.py assets.
WITH base AS (
    SELECT
        customer_key,
        customer_id,
        first_name,
        last_name,
        country,
        total_orders,
        total_sales,
        last_order_date
    FROM {{ ref('rpt_sales_summary_by_customer') }}
),
anchor AS (
    SELECT MAX(last_order_date) AS as_of_date FROM base
)
SELECT
    b.customer_key,
    b.customer_id,
    b.first_name,
    b.last_name,
    b.country,
    -- Recency: days since last order (lower = more recent)
    date_diff('day', b.last_order_date, a.as_of_date)::INTEGER AS recency_days,
    -- Frequency: number of orders placed
    b.total_orders::INTEGER AS frequency,
    -- Monetary: total sales amount
    b.total_sales::DOUBLE AS monetary,
    a.as_of_date AS features_as_of_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM base b
CROSS JOIN anchor a
WHERE b.last_order_date IS NOT NULL
