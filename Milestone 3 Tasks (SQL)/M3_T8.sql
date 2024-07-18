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

-- Adding primary key to dim_card_details
ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);

-- Adding primary key to dim_date_times
ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);

-- Adding primary key to dim_products
ALTER TABLE dim_products
ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);

-- Adding primary key to dim_store_details
ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);

-- Adding primary key to dim_users
ALTER TABLE dim_users
ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);
