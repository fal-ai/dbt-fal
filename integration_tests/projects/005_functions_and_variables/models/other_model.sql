{{ config(materialized='table') }}

WITH data AS (

    SELECT
        'some text' AS my_text,
        -- The following column will be filled in an after script with fal
        cast(NULL AS numeric) AS my_float
)

SELECT *
FROM data
