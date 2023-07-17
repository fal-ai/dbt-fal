-- depends_on: {{ ref('model_b1') }}
-- depends_on: {{ ref('model_c1') }}
-- depends_on: {{ ref('model_d1') }}

WITH data AS (
    SELECT cast(1 AS integer) AS e1_data
)

SELECT *
FROM data
