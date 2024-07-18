-- Rename the removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN "removed" TO "still_available";

-- Standardize the text values in still_available
UPDATE dim_products
SET "still_available" = CASE
    WHEN "still_available" = 'Still_avaliable' THEN 'true'
    WHEN "still_available" = 'Removed' THEN 'false'
    ELSE 'false'  -- Default to false if none of the above match
END;

-- Verify the standardized values
SELECT DISTINCT "still_available" FROM dim_products;

-- Change the still_available column from TEXT to BOOLEAN
ALTER TABLE dim_products
ALTER COLUMN "still_available" SET DATA TYPE BOOLEAN USING (
    CASE 
        WHEN "still_available" = 'true' THEN true 
        WHEN "still_available" = 'false' THEN false 
        ELSE false -- Default to false if conversion fails
    END
);

-- Change the product_price column from TEXT to FLOAT
ALTER TABLE dim_products
ALTER COLUMN "product_price" SET DATA TYPE FLOAT USING "product_price"::FLOAT;

-- Change the weight column from TEXT to FLOAT
ALTER TABLE dim_products
ALTER COLUMN "weight" SET DATA TYPE FLOAT USING "weight"::FLOAT;

-- Determine the appropriate length for the EAN and product_code columns
SELECT 
    MAX(LENGTH("EAN")) AS max_EAN_length,
    MAX(LENGTH("product_code")) AS max_product_code_length,
    MAX(LENGTH("weight_class")) AS max_weight_class_length
FROM dim_products;

-- Change the EAN column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_products
ALTER COLUMN "EAN" SET DATA TYPE VARCHAR(17);

-- Change the product_code column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_products
ALTER COLUMN "product_code" SET DATA TYPE VARCHAR(11);

-- Change the date_added column from TEXT to DATE
ALTER TABLE dim_products
ALTER COLUMN "date_added" SET DATA TYPE DATE USING "date_added"::DATE;

-- Change the uuid column from TEXT to UUID
ALTER TABLE dim_products
ALTER COLUMN "uuid" SET DATA TYPE UUID USING "uuid"::UUID;

-- Change the weight_class column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_products
ALTER COLUMN "weight_class" SET DATA TYPE VARCHAR(14);
