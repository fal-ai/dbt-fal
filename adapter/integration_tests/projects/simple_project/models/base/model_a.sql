WITH data AS (

    SELECT
        -- adapter.type() = {{ adapter.type() }}
        -- env.type = {{ env.type }}
        -- target.type = {{ target.type }}
        'some text' AS my_text
)

SELECT *
FROM data
