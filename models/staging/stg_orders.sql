select 
    id as order_id,
    user_id	as customer_id,
    order_date AS order_placed_at,
    status AS order_status
FROM {{ source('jaffle_shop', 'jaffle_shop_orders') }}