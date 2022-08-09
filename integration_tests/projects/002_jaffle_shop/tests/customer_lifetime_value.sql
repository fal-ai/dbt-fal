SELECT
    customer_id
FROM {{ ref('customers') }}
WHERE NOT (customer_lifetime_value >= 0) OR customer_lifetime_value IS NULL
