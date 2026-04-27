SELECT
    p.product_key,
    p.product_id,
    p.product_number,
    p.product_name,
    p.product_line,
    p.category,
    p.subcategory,
    p.cost,
    COUNT(f.order_number) AS total_orders,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity_sold,
    AVG(f.sales_amount) AS avg_sales_per_order,
    AVG(f.sls_price) AS avg_unit_price,
    SUM(f.quantity * p.cost) AS total_cost,
    SUM(f.sales_amount - (f.quantity * p.cost)) AS total_profit,
    MIN(f.order_date) AS first_sale_date,
    MAX(f.order_date) AS last_sale_date
FROM {{ ref('fct_sales') }} f
JOIN {{ ref('dim_products') }} p
    ON f.product_key = p.product_key
GROUP BY
    p.product_key,
    p.product_id,
    p.product_number,
    p.product_name,
    p.product_line,
    p.category,
    p.subcategory,
    p.cost
ORDER BY total_sales DESC
