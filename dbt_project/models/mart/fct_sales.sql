SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM {{ ref('stg_crm_sales_details') }} sd
LEFT JOIN {{ ref('dim_products') }} pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN {{ ref('dim_customers') }} cu
    ON sd.sls_cust_id = cu.customer_id
