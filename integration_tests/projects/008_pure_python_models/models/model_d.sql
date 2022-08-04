-- To test full-refresh
{{ config(materialized='incremental') }}

WITH data AS (
    SELECT 
        my_int,
        my_text,
        my_bool,
        cast('2022-05-11' AS date) AS my_date
    FROM {{ ref('model_c') }}
)

SELECT *
FROM data
