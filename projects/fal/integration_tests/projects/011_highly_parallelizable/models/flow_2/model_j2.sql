-- depends_on: {{ ref('model_g2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS j2_data
)

SELECT *
FROM data
