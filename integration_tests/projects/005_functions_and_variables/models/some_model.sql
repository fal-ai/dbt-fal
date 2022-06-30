{{ config(materialized='table') }}

WITH data AS (
    SELECT 
        1.2 AS my_float,
        my_text, 
        -- the after script value should reflect here
        my_int
    FROM {{ ref('other_model') }}
)

SELECT *
FROM data
