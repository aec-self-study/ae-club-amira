{{ config(materialized='table') }}

SELECT
    date_trunc(first_order_at, month) AS signup_month,
    COUNT(*) AS new_customers
FROM {{ ref('customers') }}
GROUP BY 1
