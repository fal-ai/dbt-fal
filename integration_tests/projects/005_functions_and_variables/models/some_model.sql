{{ config(materialized='table') }}

WITH data AS (
    SELECT 
        cast(1 AS integer) AS my_int,
        my_text, 
        -- the after script value should reflect here
        my_float
    FROM {{ ref('other_model') }}
)

SELECT *
FROM data
