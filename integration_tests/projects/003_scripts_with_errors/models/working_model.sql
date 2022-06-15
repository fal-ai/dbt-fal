{{ config(materialized='table') }}

WITH data AS (
    SELECT 
        cast(1 AS integer) AS my_int
)

SELECT *
FROM data
