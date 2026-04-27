{{
    config(
        materialized='incremental',
        unique_key='snapshot_date',
        incremental_strategy='delete+insert',
    )
}}

-- Customer RFM features for the current partition. Anchor date is the
-- partition date itself (i.e. "as-of this snapshot") rather than the max
-- order date in the data, so recency is computed deterministically day by
-- day. Owned by ml_team (see +group in dbt_project.yml); consumed by
-- ml_pipelines' segmentation and churn assets.
SELECT
    b.customer_key,
    b.customer_id,
    b.first_name,
    b.last_name,
    b.country,
    -- Recency: days between the partition date (as-of) and last order.
    date_diff('day', b.last_order_date, '{{ var("snapshot_dt") }}'::DATE)::INTEGER AS recency_days,
    -- Frequency: number of orders placed within the partition.
    b.total_orders::INTEGER AS frequency,
    -- Monetary: total sales amount within the partition.
    b.total_sales::DOUBLE AS monetary,
    '{{ var("snapshot_dt") }}'::DATE AS features_as_of_date,
    '{{ var("snapshot_dt") }}'::DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('rpt_sales_summary_by_customer') }} b
WHERE b.snapshot_date = '{{ var("snapshot_dt") }}'::DATE
  AND b.last_order_date IS NOT NULL
