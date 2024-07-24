SELECT
    SUM(o.product_quantity * dp.product_price) AS total_sales,
    d.month
FROM orders_table o
JOIN dim_products dp ON o.product_code = dp.product_code
JOIN dim_date_times d ON o.date_uuid = d.date_uuid
WHERE o.product_quantity > 0 
  AND dp.product_price > 0
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6;
