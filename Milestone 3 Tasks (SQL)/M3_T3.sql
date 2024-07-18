/*
    Step 3: Alter the table to change data types.
    The ALTER TABLE statements below will modify the data types of the specified columns 
    based on the required data types provided in the task description.
*/

/*
    Alter the data type of the longitude column from TEXT to REAL.
*/
ALTER TABLE dim_store_details
ALTER COLUMN longitude SET DATA TYPE REAL USING longitude::REAL;

/*
    Alter the data type of the locality column from TEXT to VARCHAR(255).
*/
ALTER TABLE dim_store_details
ALTER COLUMN locality SET DATA TYPE VARCHAR(255);

/*
    Based on the maximum length found, alter the data type of the store_code column.
    Assuming a length of 12 for store_code as an example. Adjust based on the query result.
*/
ALTER TABLE dim_store_details
ALTER COLUMN store_code SET DATA TYPE VARCHAR(12);

/*
    Alter the data type of the staff_numbers column from TEXT to SMALLINT.
    We use the USING clause to specify the conversion from TEXT to SMALLINT.
*/
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT;

/*
    Alter the data type of the opening_date column from TEXT to DATE.
    We use the USING clause to specify the conversion from TEXT to DATE.
*/
ALTER TABLE dim_store_details
ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::DATE;

/*
    Alter the data type of the store_type column from TEXT to VARCHAR(255) and make it nullable.
*/
ALTER TABLE dim_store_details
ALTER COLUMN store_type SET DATA TYPE VARCHAR(255);
ALTER TABLE dim_store_details
ALTER COLUMN store_type DROP NOT NULL;

/*
    Alter the data type of the latitude column from TEXT to REAL.
*/
ALTER TABLE dim_store_details
ALTER COLUMN latitude SET DATA TYPE REAL USING latitude::REAL;

/*
    Based on the maximum length found, alter the data type of the country_code column.
    Assuming a length of 2 for country_code as an example. Adjust based on the query result.
*/
ALTER TABLE dim_store_details
ALTER COLUMN country_code SET DATA TYPE VARCHAR(2);

/*
    Alter the data type of the continent column from TEXT to VARCHAR(255).
*/
ALTER TABLE dim_store_details
ALTER COLUMN continent SET DATA TYPE VARCHAR(255);

/*
    Update the row at index 0 to set the values of longitude, latitude, address, and locality to NULL.
*/
UPDATE dim_store_details
SET longitude = NULL,
    latitude = NULL,
    address = NULL,
    locality = NULL
WHERE index = 0;
