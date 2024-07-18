/*
    Step 2: Alter the table to change data types.
    The ALTER TABLE statements below will modify the data types of the specified columns 
    based on the required data types provided in the task description.
*/

/*
    Alter the data type of the first_name column from TEXT to VARCHAR(255).
*/
ALTER TABLE dim_users
ALTER COLUMN first_name SET DATA TYPE VARCHAR(255);

/*
    Alter the data type of the last_name column from TEXT to VARCHAR(255).
*/
ALTER TABLE dim_users
ALTER COLUMN last_name SET DATA TYPE VARCHAR(255);

/*
    Alter the data type of the date_of_birth column from TEXT to DATE.
    We use the USING clause to specify the conversion from TEXT to DATE.
*/
ALTER TABLE dim_users
ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::DATE;

/*
    Determine the maximum length for the country_code column if not already known.
    This step is useful to ensure the new VARCHAR length is sufficient.
*/
SELECT MAX(LENGTH(country_code::TEXT)) AS max_country_code_length
FROM dim_users;

/*
    Based on the maximum length found, alter the data type of the country_code column.
    Assuming a length of 3 for country_code as an example. Adjust based on the query result.
*/
ALTER TABLE dim_users
ALTER COLUMN country_code SET DATA TYPE VARCHAR(3);

/*
    Alter the data type of the user_uuid column from TEXT to UUID.
    We use the USING clause to specify the conversion from TEXT to UUID.
*/
ALTER TABLE dim_users
ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID;

/*
    Alter the data type of the join_date column from TEXT to DATE.
    We use the USING clause to specify the conversion from TEXT to DATE.
*/
ALTER TABLE dim_users
ALTER COLUMN join_date SET DATA TYPE DATE USING join_date::DATE;
