{{ config(materialized='table') }}

WITH sales AS (
    SELECT
      order_items.id AS sale_id,
      date_trunc(orders.created_at, day) AS order_date,
      orders.customer_id,
      products.category AS product_category,
      product_prices.price AS revenue
FROM {{ source('coffee_shop', 'order_items') }} AS order_items
LEFT JOIN
    {{ source('coffee_shop', 'orders') }} AS orders
LEFT JOIN
    {{ source('coffee_shop', 'products') }} AS products
LEFT JOIN
    {{ source('coffee_shop', 'product_prices') }} AS product_prices
    ON products.id = product_prices.product_id
        AND orders.created_at >= product_prices.created_at
)

SELECT * FROM sales