-- Gold fact: one row per order line, revenue + margin measures
SELECT o.order_id, o.customer_id, o.product_id, o.city, o.quantity, o.unit_price, o.quantity*o.unit_price AS revenue FROM {{ ref('stg_orders') }} AS o
