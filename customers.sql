WITH customer_orders AS (
  SELECT 
  customer_id
  ,MIN(created_at) AS first_order_at
  ,COUNT(*) AS number_of_orders
  FROM 
  GROUP BY 1
  )

 SELECT
  customers.id as customer_id
  , customers.name
  , customers.email
  , COALESCE(customer_orders.number_of_orders, 0) as number_of_orders
  , customer_orders.first_order_at
FROM  as customers
LEFT JOIN  customer_orders
  ON  customers.id = customer_orders.customer_id 

LIMIT 5
