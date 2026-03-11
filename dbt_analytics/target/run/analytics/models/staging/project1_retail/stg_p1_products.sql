

  create or replace view `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_p1_products`
  OPTIONS()
  as 

select
    product_id,
    product_name,
    category,
    price,
    created_at,
    updated_at
from `jcdeah-007.finalproject_rakhajidhan_ecommerce_retails.products`
where product_id is not null;

