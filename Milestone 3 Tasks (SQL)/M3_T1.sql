/*
    Determine the maximum lengths for VARCHAR fields.
    This will help us set appropriate maximum lengths for the VARCHAR columns.
*/

-- Check the maximum lengths for VARCHAR fields

SELECT 
    MAX(LENGTH(card_number::TEXT)) AS max_card_number_length,
    MAX(LENGTH(store_code::TEXT)) AS max_store_code_length,
    MAX(LENGTH(product_code::TEXT)) AS max_product_code_length
FROM orders_table;

/*
    Step 2: Alter the table to change data types.
    The ALTER TABLE statements below will modify the data types of the specified columns 
    based on the required data types provided in the task description.
*/

/*
    Alter the data type of the date_uuid column from TEXT to UUID.
    We use the USING clause to specify the conversion from TEXT to UUID.
*/
ALTER TABLE orders_table
ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID;

/*
    Alter the data type of the user_uuid column from TEXT to UUID.
    Similarly, we use the USING clause for the conversion.
*/
ALTER TABLE orders_table
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID;

/*
    Alter the data type of the card_number column from TEXT to VARCHAR.
    We assume the maximum length of card_number is 16 characters.
    Adjust the length based on the results from the previous query if needed.
*/
ALTER TABLE orders_table
ALTER COLUMN card_number SET DATA TYPE VARCHAR(19);

/*
    Alter the data type of the store_code column from TEXT to VARCHAR.
    We assume the maximum length of store_code is 10 characters.
    Adjust the length based on the results from the previous query if needed.
*/
ALTER TABLE orders_table
ALTER COLUMN store_code SET DATA TYPE VARCHAR(12);

/*
    Alter the data type of the product_code column from TEXT to VARCHAR.
    We assume the maximum length of product_code is 20 characters.
    Adjust the length based on the results from the previous query if needed.
*/
ALTER TABLE orders_table
ALTER COLUMN product_code SET DATA TYPE VARCHAR(11);

/*
    Alter the data type of the product_quantity column from BIGINT to SMALLINT.
    We use the USING clause to specify the conversion from BIGINT to SMALLINT.
*/
ALTER TABLE orders_table
ALTER COLUMN product_quantity SET DATA TYPE SMALLINT USING product_quantity::SMALLINT;
