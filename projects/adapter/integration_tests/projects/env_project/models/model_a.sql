{{ config(materialized='table') }}
WITH data AS (

    SELECT
        'some text' AS my_text
)

SELECT *
FROM data
