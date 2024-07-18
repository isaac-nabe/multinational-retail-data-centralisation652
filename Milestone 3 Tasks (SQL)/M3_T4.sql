/*
    Step 1: Remove the £ character from the product_price column and convert it to a numeric type.
*/

/* 
    Update the product_price column to remove the £ character.
    Assuming the price is stored as text, we will first remove the £ character and then cast to numeric.
*/
UPDATE dim_products
SET product_price = TRIM(BOTH '£' FROM product_price);

/* 
    Alter the column data type to numeric (e.g., DECIMAL).
    Adjust precision and scale as necessary, here assumed as DECIMAL(10, 2).
*/
ALTER TABLE dim_products
ALTER COLUMN product_price SET DATA TYPE DECIMAL(10, 2) USING product_price::DECIMAL;

/*
    Step 2: Add the weight_class column to the dim_products table and update its values based on the weight range.
*/

/* 
    Add the new weight_class column to the dim_products table.
    Assuming a maximum length of 20 for VARCHAR based on the provided weight classes.
*/
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

/*
    Update the weight_class column based on the weight range.
*/
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;
