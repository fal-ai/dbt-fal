-- depends_on: {{ ref('model_d2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS g2_data
)

SELECT *
FROM data
