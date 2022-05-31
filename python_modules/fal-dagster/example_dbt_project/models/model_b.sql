WITH data AS (
    SELECT 
        cast(1 AS integer) AS my_int,
        my_text
    FROM {{ ref('model_a') }}
)

SELECT *
FROM data
