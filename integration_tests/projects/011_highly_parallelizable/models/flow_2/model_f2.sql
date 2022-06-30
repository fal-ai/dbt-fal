-- depends_on: {{ ref('model_c2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS f2_data
)

SELECT *
FROM data
