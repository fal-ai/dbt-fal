-- depends_on: {{ ref('model_b2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS e2_data
)

SELECT *
FROM data
