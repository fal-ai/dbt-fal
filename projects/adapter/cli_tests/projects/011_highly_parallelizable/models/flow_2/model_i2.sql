-- depends_on: {{ ref('model_f2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS i2_data
)

SELECT *
FROM data
