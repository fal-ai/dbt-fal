-- depends_on: {{ ref('model_e1') }}
-- depends_on: {{ ref('model_h2') }}
-- depends_on: {{ ref('model_i2') }}
-- depends_on: {{ ref('model_j2') }}

WITH data AS (
    SELECT cast(1 AS integer) AS final_data
)

SELECT *
FROM data
