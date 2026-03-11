

  create or replace view `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_p1_purchases`
  OPTIONS()
  as 

select
    purchase_id,
    customer_id,
    product_id,
    quantity,
    created_at,
    updated_at
from `jcdeah-007.finalproject_rakhajidhan_ecommerce_retails.purchase`
where purchase_id is not null;

