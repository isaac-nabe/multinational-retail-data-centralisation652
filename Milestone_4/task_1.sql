-- Query to count the number of stores per country, excluding web portals

-- Select country code and count of stores
SELECT
    -- Rename country_code to country for the result set
    country_code AS country,
    -- Count the number of stores and rename the result to total_no_stores
    COUNT(*) AS total_no_stores
FROM
    -- Source table containing store details
    dim_store_details
WHERE
    -- Exclude rows where the store_type is 'Web Portal'
    store_type != 'Web Portal'
GROUP BY
    -- Group the results by country_code to aggregate the store counts
    country_code
ORDER BY
    -- Order the results by total_no_stores in descending order
    total_no_stores DESC;
