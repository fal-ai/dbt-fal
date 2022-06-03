{{ config(materialized='table') }}
-- {{ ref("execute_sql_model_two") }}

WITH data AS (
    SELECT
        'some text' AS my_text
)

SELECT *
FROM data
