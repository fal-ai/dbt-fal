-- depends_on: {{ ref('regular_model') }}

WITH data AS (
    SELECT
        BROKEN_MODEL,
    FROM {{ ref('get_data') }}
)

SELECT *
FROM data
