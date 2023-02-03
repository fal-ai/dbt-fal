-- depends_on: {{ ref('model_a2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS d2_data
)

SELECT *
FROM data
