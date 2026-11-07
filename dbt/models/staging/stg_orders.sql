-- staging: rename + cast only, no business logic
SELECT CAST(order_id AS BIGINT) AS order_id, CAST(customer_id AS BIGINT) AS customer_id, CAST(product_id AS BIGINT) AS product_id, city, CAST(quantity AS INT) AS quantity, CAST(unit_price AS DECIMAL(10,2)) AS unit_price, created_at FROM {{ source('bronze','orders') }}
