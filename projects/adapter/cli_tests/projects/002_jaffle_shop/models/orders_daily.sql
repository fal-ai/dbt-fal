{% set order_statuses = ['returned', 'completed', 'return_pending', 'shipped', 'placed'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

final as (

    select
        orders.order_date,
        count(*) as order_count,

        {% for order_status in order_statuses -%}
        sum(case when orders.status = '{{ order_status }}' then 1 else 0 end) as {{ order_status }}_status,
        {% endfor %}

        sum(payments.amount) as order_amount
    from orders


    left join payments
        on orders.order_id = payments.order_id
    
    group by orders.order_date

)

select * from final
