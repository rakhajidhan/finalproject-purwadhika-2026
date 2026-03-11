

  create or replace view `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_p3_transactions`
  OPTIONS()
  as 

select
    transaction_id,
    event_type,
    customer_id,
    customer_name,
    customer_city,
    product_id,
    product_name,
    product_category,
    unit_price,
    quantity,
    total_amount,
    payment_method,
    status,
    published_at,
    ingested_at,
    run_date
from `jcdeah-007.finalproject_rakhajidhan_pubsub_retail.retail_transactions_streaming`
where transaction_id is not null;

