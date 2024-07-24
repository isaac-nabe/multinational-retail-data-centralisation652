-- This Common Table Expression (CTE) calculates the total number of sales across all store types.
WITH total_sales_count AS (
    SELECT 
        COUNT(*) AS total_count -- Count the total number of sales
    FROM 
        orders_table o
    JOIN 
        dim_store_details sd ON o.store_code = sd.store_code -- Join with store details to get store information
    JOIN 
        dim_products p ON o.product_code = p.product_code -- Join with products to get product information
)
-- The main query selects store type, total sales, and percentage of sales for each store type.
SELECT 
    sd.store_type, -- Select the store type
    ROUND(SUM(o.product_quantity * p.product_price)::numeric, 2) AS total_sales, -- Calculate and round total sales to 2 decimal points
    ROUND((COUNT(*)::numeric / ts.total_count) * 100, 2) AS sale_percentage -- Calculate and round the percentage of total sales to 2 decimal points
FROM 
    orders_table o
JOIN 
    dim_store_details sd ON o.store_code = sd.store_code -- Join with store details to get store type
JOIN 
    dim_products p ON o.product_code = p.product_code -- Join with products to get product price
CROSS JOIN 
    total_sales_count ts -- Cross join with the CTE to get the total sales count for percentage calculation
GROUP BY 
    sd.store_type, ts.total_count -- Group by store type and total count to aggregate the results correctly
ORDER BY 
    sale_percentage DESC; -- Order the results by sale percentage in descending order
