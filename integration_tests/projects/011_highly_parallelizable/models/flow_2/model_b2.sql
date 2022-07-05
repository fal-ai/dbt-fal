-- depends_on: {{ ref('model_a2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS b2_data
)

SELECT *
FROM data
