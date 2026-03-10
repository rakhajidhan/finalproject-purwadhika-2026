{{ config(materialized='table') }}

select
    p.purchase_id,
    c.customer_id,
    c.name as customer_name,
    c.city as customer_city,
    c.email as customer_email,
    pr.product_id,
    pr.product_name,
    pr.category as product_category,
    pr.price as unit_price,
    p.quantity,
    cast(pr.price as float64) * cast(p.quantity as float64) as total_amount,
    p.created_at as purchase_date,
    current_timestamp() as dbt_loaded_at
from `jcdeah-007.finalproject_rakhajidhan_ecommerce_retails.purchase` p
inner join `jcdeah-007.finalproject_rakhajidhan_ecommerce_retails.customers` c
    on p.customer_id = c.customer_id
inner join `jcdeah-007.finalproject_rakhajidhan_ecommerce_retails.products` pr
    on p.product_id = pr.product_id
