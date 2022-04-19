{{ config(materialized='table') }}

WITH data AS (
    SELECT 
        1::integer AS my_int,
        my_text, 
        -- the after script value should reflect here
        my_float
    FROM {{ ref('other_model') }}
)

SELECT *
FROM data
