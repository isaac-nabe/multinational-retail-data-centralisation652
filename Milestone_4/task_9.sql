-- Step 1: Extract and order timestamps
WITH date_times AS (
    SELECT
        year,  -- Select the year
        timestamp as times  -- Select the timestamp and alias it as 'times'
    FROM dim_date_times d
    JOIN orders_table o ON d.date_uuid = o.date_uuid  -- Join with orders_table on date_uuid
    JOIN dim_store_details s ON o.store_code = s.store_code  -- Join with dim_store_details on store_code
    ORDER BY times DESC  -- Order by the timestamp in descending order
),

-- Step 2: Calculate the next timestamp for each record
next_times AS (
    SELECT 
        year,  -- Select the year
        times,  -- Select the timestamp
        LEAD(times) OVER (ORDER BY times DESC) AS next_times  -- Get the next timestamp using LEAD function
    FROM date_times
),

-- Step 3: Calculate the average time difference in seconds
avg_times AS (
    SELECT 
        year,  -- Select the year
        AVG(EXTRACT(EPOCH FROM (times - next_times))) AS avg_times_seconds  -- Calculate the average time difference in seconds
    FROM next_times
    GROUP BY year  -- Group by year
    ORDER BY avg_times_seconds DESC  -- Order by the average time difference in descending order
)

-- Step 4: Format the average time difference into hours, minutes, seconds, and milliseconds
SELECT 
    year,  -- Select the year
    CONCAT(
        '"Hours": ', FLOOR(avg_times_seconds / 3600), ', ',  -- Convert total seconds to hours and round down
        '"Minutes": ', FLOOR((avg_times_seconds % 3600) / 60), ', ',  -- Convert remaining seconds to minutes and round down
        '"Seconds": ', FLOOR(avg_times_seconds % 60), ', ',  -- Get the remaining seconds and round down
        '"Milliseconds": ', FLOOR((avg_times_seconds - FLOOR(avg_times_seconds)) * 1000)  -- Calculate milliseconds and round down
    ) as actual_time_taken  -- Alias the concatenated result as 'actual_time_taken'
FROM avg_times
ORDER BY avg_times_seconds DESC  -- Order by the average time difference in descending order
LIMIT 5;  -- Limit the result to the top 5 records
