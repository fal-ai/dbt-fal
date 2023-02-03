-- {{ ref("model_a") }}
-- {{ ref("model_b") }}
select y from {{ ref('time_series') }}
