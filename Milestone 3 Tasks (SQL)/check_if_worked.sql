--View the current structure and data types of the table
--SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'dim_products';

--SELECT *
--FROM dim_products

-- Verify the column names and data types
--SELECT column_name, data_type
--FROM information_schema.columns
--WHERE table_name = 'dim_card_details';

-- Check if primary keys added correctly
/*
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'dim_users'
    AND CONSTRAINT_NAME = 'pk_user_uuid';
*/


-- Check if Foreign keys were added correctly
/*
-- Check foreign key on card_number
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'orders_table'
    AND CONSTRAINT_NAME = 'fk_orders_card_number';

-- Check foreign key on date_uuid
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'orders_table'
    AND CONSTRAINT_NAME = 'fk_orders_date_uuid';

-- Check foreign key on product_code
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'orders_table'
    AND CONSTRAINT_NAME = 'fk_orders_product_code';

-- Check foreign key on store_code
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'orders_table'
    AND CONSTRAINT_NAME = 'fk_orders_store_code';

-- Check foreign key on user_uuid
SELECT
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME
FROM
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE
    TABLE_NAME = 'orders_table'
    AND CONSTRAINT_NAME = 'fk_orders_user_uuid';
*/

--SELECT COUNT(*) FROM dim_card_details;

SELECT DISTINCT card_number
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);
