WITH data AS (
    SELECT
        cast(1 AS integer) AS c1_data
    FROM {{ ref('model_a1') }}
)

SELECT *
FROM data
