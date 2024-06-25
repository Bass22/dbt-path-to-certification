SELECT 
    ID,
    ORDERID,
    PAYMENTMETHOD,
    STATUS,
    AMOUNT,
    CREATED
FROM {{ source('jaffle_shop', 'stripe_payments') }}