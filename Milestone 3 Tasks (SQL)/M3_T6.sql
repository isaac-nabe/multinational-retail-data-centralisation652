-- Ensure columns are of type TEXT
/*ALTER TABLE dim_date_times
ALTER COLUMN "month" SET DATA TYPE TEXT USING "month"::TEXT;

ALTER TABLE dim_date_times
ALTER COLUMN "year" SET DATA TYPE TEXT USING "year"::TEXT;

ALTER TABLE dim_date_times
ALTER COLUMN "day" SET DATA TYPE TEXT USING "day"::TEXT;

ALTER TABLE dim_date_times
ALTER COLUMN "time_period" SET DATA TYPE TEXT USING "time_period"::TEXT;

-- Determine the maximum lengths for VARCHAR fields
SELECT 
    MAX(LENGTH("month")) AS max_month_length,
    MAX(LENGTH("year")) AS max_year_length,
    MAX(LENGTH("day")) AS max_day_length,
    MAX(LENGTH("time_period")) AS max_time_period_length
FROM dim_date_times;
*/
-- Change the month column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_date_times
ALTER COLUMN "month" SET DATA TYPE VARCHAR(2);

-- Change the year column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_date_times
ALTER COLUMN "year" SET DATA TYPE VARCHAR(4);

-- Change the day column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_date_times
ALTER COLUMN "day" SET DATA TYPE VARCHAR(2);

-- Change the time_period column from TEXT to VARCHAR with an appropriate length
ALTER TABLE dim_date_times
ALTER COLUMN "time_period" SET DATA TYPE VARCHAR(10);

-- Change the date_uuid column from TEXT to UUID
ALTER TABLE dim_date_times
ALTER COLUMN "date_uuid" SET DATA TYPE UUID USING "date_uuid"::UUID;
