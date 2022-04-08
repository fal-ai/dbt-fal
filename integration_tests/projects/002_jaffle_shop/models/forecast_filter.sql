WITH orders_forecast AS (

    SELECT 
        *
    FROM {{ ref('orders_forecast') }}

), final AS (

    SELECT
        ds AS forecast_date,
        yhat_count AS forecast_count,
        yhat_amount AS forecast_amount
    FROM orders_forecast
    WHERE yhat_amount > 0

)

SELECT *
FROM final
