{{ config(materialized='table') }}

select
    p.purchase_id,
    c.customer_id,
    c.name         as customer_name,
    c.city         as customer_city,
    c.email        as customer_email,
    pr.product_id,
    pr.product_name,
    pr.category    as product_category,
    pr.price       as unit_price,
    p.quantity,
    cast(pr.price as float64) * cast(p.quantity as float64) as total_amount,
    p.created_at   as purchase_date,
    current_timestamp() as dbt_loaded_at
from {{ ref('stg_p1_purchases') }} p
inner join {{ ref('stg_p1_customers') }} c
    on p.customer_id = c.customer_id
inner join {{ ref('stg_p1_products') }} pr
    on p.product_id = pr.product_id
