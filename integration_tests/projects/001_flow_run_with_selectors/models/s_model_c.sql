-- {{ ref("s_model_a") }}
-- {{ ref("s_model_b") }}
select y from {{ ref('time_series') }}
