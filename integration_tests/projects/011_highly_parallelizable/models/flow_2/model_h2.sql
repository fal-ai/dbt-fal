-- depends_on: {{ ref('model_e2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS h2_data
)

SELECT *
FROM data
