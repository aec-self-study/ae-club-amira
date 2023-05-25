{{ config(materialized='table') }}

WITH customers AS (
	SELECT 
	orders.customer_id AS customer_id,
	customers.name AS name,
	customers.email AS email,
	MIN(orders.created_at) AS first_order_at,
	COUNT(orders.created_at) AS nr_of_orders
FROM {{ source('coffee_shop', 'customers') }} AS customers
LEFT JOIN {{ source('coffee_shop', 'orders')}} AS orders ON customers.id=orders.customer_id
GROUP BY 1, 2, 3 
)

SELECT * FROM customers