SELECT
    sales.year,
    sales.month,
    ROUND(sales.total_sales::NUMERIC, 2) AS total_sales
FROM (
    SELECT
        dt.year,
        dt.month,
        SUM(o.product_quantity * p.product_price) AS total_sales
    FROM orders_table o
    JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
    JOIN dim_products p ON o.product_code = p.product_code
    GROUP BY dt.year, dt.month
) sales
ORDER BY sales.total_sales DESC
LIMIT 10;
