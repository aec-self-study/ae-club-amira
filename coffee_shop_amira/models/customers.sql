{{ config(materialized='table') }}

SELECT 
	orders.customer_id AS customer_id,
	customers.name AS name,
	customers.email AS email,
	MIN(orders.created_at) AS first_order_at,
	COUNT(orders.created_at) AS nr_of_orders
FROM `analytics-engineers-club.coffee_shop.customers` AS customers
LEFT JOIN `analytics-engineers-club.coffee_shop.orders` AS orders ON customers.id=orders.customer_id
GROUP BY 1, 2, 3 