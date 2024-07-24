-- Query to count the number of stores per locality, excluding web portals

-- Select locality and count of stores
SELECT
    -- Rename locality to locality for the result set
    locality,
    -- Count the number of stores and rename the result to total_no_stores
    COUNT(*) AS total_no_stores
FROM
    -- Source table containing store details
    dim_store_details
WHERE
    -- Exclude rows where the store_type is 'Web Portal'
    store_type != 'Web Portal'
GROUP BY
    -- Group the results by locality to aggregate the store counts
    locality
ORDER BY
    -- Order the results by total_no_stores in descending order
    total_no_stores DESC
-- Limit the results to the top 7 localities
LIMIT 7;
