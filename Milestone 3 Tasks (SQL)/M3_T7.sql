-- Ensure columns are of type TEXT
/*
ALTER TABLE dim_card_details
ALTER COLUMN card_number SET DATA TYPE TEXT USING card_number::TEXT;
*/

/*ALTER TABLE dim_card_details
ALTER COLUMN expiry_date SET DATA TYPE TEXT USING expiry_date::TEXT;

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE USING date_payment_confirmed::DATE;
*/

-- Determine the maximum lengths for VARCHAR fields

--SELECT
    --MAX(LENGTH(card_number)) AS max_card_number_length,
    --MAX(LENGTH(expiry_date)) AS max_expiry_date_length
--FROM dim_card_details;



-- Change the card_number column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_card_details
ALTER COLUMN "card_number" SET DATA TYPE VARCHAR(19);

-- Set the card_number column to NOT NULL
ALTER TABLE dim_card_details
ALTER COLUMN "card_number" SET NOT NULL;

-- Change the card_number column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_card_details
ALTER COLUMN "expiry_date" SET DATA TYPE VARCHAR(19);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed SET DATA TYPE DATE USING date_payment_confirmed::DATE;
