{{ config(materialized='table', alias='third') }}

WITH data AS (

    SELECT
        'some text' AS my_text,
        -- The following column will be filled in an after script with fal
        cast(NULL AS integer) AS my_int
)

SELECT *
FROM data
