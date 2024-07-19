-- Task 1: Update the orders_table columns to the correct data types
-- Determine the maximum lengths for VARCHAR fields

SELECT 
    MAX(LENGTH(card_number::TEXT)) AS max_card_number_length,
    MAX(LENGTH(store_code::TEXT)) AS max_store_code_length,
    MAX(LENGTH(product_code::TEXT)) AS max_product_code_length
FROM orders_table;

-- Alter columns in orders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number SET DATA TYPE VARCHAR(19),
ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
ALTER COLUMN product_code SET DATA TYPE VARCHAR(11),
ALTER COLUMN product_quantity SET DATA TYPE SMALLINT USING product_quantity::SMALLINT;

-- Task 2: Update the dim_users table columns to the correct data types
ALTER TABLE dim_users
ALTER COLUMN first_name SET DATA TYPE VARCHAR(255),
ALTER COLUMN last_name SET DATA TYPE VARCHAR(255),
ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date SET DATA TYPE DATE USING join_date::DATE;

-- Determine the maximum length for the country_code column
SELECT MAX(LENGTH(country_code::TEXT)) AS max_country_code_length
FROM dim_users;

-- Alter the country_code column based on the maximum length found
ALTER TABLE dim_users
ALTER COLUMN country_code SET DATA TYPE VARCHAR(3);

-- Task 3: Update the dim_store_details table columns to the correct data types
ALTER TABLE dim_store_details
ALTER COLUMN longitude SET DATA TYPE REAL USING longitude::REAL,
ALTER COLUMN locality SET DATA TYPE VARCHAR(255),
ALTER COLUMN store_code SET DATA TYPE VARCHAR(12),
ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
ALTER COLUMN store_type DROP NOT NULL,
ALTER COLUMN latitude SET DATA TYPE REAL USING latitude::REAL,
ALTER COLUMN country_code SET DATA TYPE VARCHAR(2),
ALTER COLUMN continent SET DATA TYPE VARCHAR(255);

-- Update the row at index 0 to set the values to NULL
UPDATE dim_store_details
SET longitude = NULL,
    latitude = NULL,
    address = NULL,
    locality = NULL
WHERE index = 0;

-- Task 4: Make changes to the dim_products table for the delivery team
-- Remove the £ character from product_price and convert to DECIMAL
UPDATE dim_products
SET product_price = TRIM(BOTH '£' FROM product_price);

ALTER TABLE dim_products
ALTER COLUMN product_price SET DATA TYPE DECIMAL(10, 2) USING product_price::DECIMAL;

-- Add weight_class column and update its values based on the weight range
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
END;

-- Task 5: Update the dim_products table with the required data types
-- Rename removed column to still_available and standardize values
ALTER TABLE dim_products
RENAME COLUMN "removed" TO "still_available";

UPDATE dim_products
SET "still_available" = CASE
    WHEN "still_available" = 'Still_avaliable' THEN 'true'
    WHEN "still_available" = 'Removed' THEN 'false'
    ELSE 'false'
END;

ALTER TABLE dim_products
ALTER COLUMN "still_available" SET DATA TYPE BOOLEAN USING (
    CASE 
        WHEN "still_available" = 'true' THEN true 
        WHEN "still_available" = 'false' THEN false 
        ELSE false
    END
);

-- Change other columns in dim_products
ALTER TABLE dim_products
ALTER COLUMN "product_price" SET DATA TYPE FLOAT USING "product_price"::FLOAT,
ALTER COLUMN "weight" SET DATA TYPE FLOAT USING "weight"::FLOAT,
ALTER COLUMN "EAN" SET DATA TYPE VARCHAR(17),
ALTER COLUMN "product_code" SET DATA TYPE VARCHAR(11),
ALTER COLUMN "date_added" SET DATA TYPE DATE USING "date_added"::DATE,
ALTER COLUMN "uuid" SET DATA TYPE UUID USING "uuid"::UUID,
ALTER COLUMN "weight_class" SET DATA TYPE VARCHAR(14);

-- Task 6: Update the dim_date_times table with the correct types
ALTER TABLE dim_date_times
ALTER COLUMN "month" SET DATA TYPE VARCHAR(2),
ALTER COLUMN "year" SET DATA TYPE VARCHAR(4),
ALTER COLUMN "day" SET DATA TYPE VARCHAR(2),
ALTER COLUMN "time_period" SET DATA TYPE VARCHAR(10),
ALTER COLUMN "date_uuid" SET DATA TYPE UUID USING "date_uuid"::UUID;

-- Task 7: Update the dim_card_details table
ALTER TABLE dim_card_details
ALTER COLUMN "card_number" SET DATA TYPE VARCHAR(19),
ALTER COLUMN "card_number" SET NOT NULL,
ALTER COLUMN "expiry_date" SET DATA TYPE VARCHAR(19),
ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE USING date_payment_confirmed::DATE;

-- Task 8: Create the primary keys in the dimension tables
ALTER TABLE dim_card_details ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);
ALTER TABLE dim_date_times ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);
ALTER TABLE dim_products ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);
ALTER TABLE dim_store_details ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);
ALTER TABLE dim_users ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);

-- Task 9: Finalize the star-based schema by adding the foreign keys to the orders table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
ADD CONSTRAINT fk_orders_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD CONSTRAINT fk_orders_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD CONSTRAINT fk_orders_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD CONSTRAINT fk_orders_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);


/*
-- If Neccessary, you can use the below lines to drop existing constraints if you need to edit anything and re-upload to your `sales_data` database.

-- Drop existing primary key constraints if they exist
ALTER TABLE dim_card_details DROP CONSTRAINT IF EXISTS pk_card_number;
ALTER TABLE dim_date_times DROP CONSTRAINT IF EXISTS pk_date_uuid;
ALTER TABLE dim_products DROP CONSTRAINT IF EXISTS pk_product_code;
ALTER TABLE dim_store_details DROP CONSTRAINT IF EXISTS pk_store_code;
ALTER TABLE dim_users DROP CONSTRAINT IF EXISTS pk_user_uuid;

-- Drop the foreign key constraints
ALTER TABLE orders_table DROP CONSTRAINT IF EXISTS fk_orders_store_code;
ALTER TABLE orders_table DROP CONSTRAINT IF EXISTS fk_orders_card_number;
ALTER TABLE orders_table DROP CONSTRAINT IF EXISTS fk_orders_date_uuid;
ALTER TABLE orders_table DROP CONSTRAINT IF EXISTS fk_orders_product_code;
ALTER TABLE orders_table DROP CONSTRAINT IF EXISTS fk_orders_user_uuid;
*/