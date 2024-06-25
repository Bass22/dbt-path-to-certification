WITH success_payments as (
    select 
        ORDERID as order_id, 
        max(CREATED) as payment_finalized_date, 
        sum(AMOUNT) / 100.0 as total_amount_paid
    from {{ ref('stg_payments') }}
    where STATUS <> 'fail'
    group by 1
),

paid_orders as (
    select
        Orders.*,
        p.total_amount_paid,
        p.payment_finalized_date
    FROM {{ ref('stg_orders') }} as Orders
    left join success_payments p ON Orders.order_id = p.order_id
),

final as (
    select 
        po.*,
        C.customer_first_name,
        C.customer_last_name
    FROM paid_orders po
    left join {{ ref('stg_customers') }} C on po.customer_id = C.customer_id 
)

select * from final