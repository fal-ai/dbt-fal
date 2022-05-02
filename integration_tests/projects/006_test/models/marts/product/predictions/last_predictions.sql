-- {{ ref("features_input") }}
WITH ranked AS (
    SELECT
        my_int
    FROM {{ source('fal', 'all_predictions') }}
)

SELECT
    my_int AS user_id,
    my_int AS prediction
FROM ranked
WHERE my_int = 1
