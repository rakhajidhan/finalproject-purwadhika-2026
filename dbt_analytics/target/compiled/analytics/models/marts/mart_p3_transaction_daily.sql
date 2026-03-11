

select
    run_date,
    customer_id,
    customer_name,
    customer_city,
    product_category,
    payment_method,
    status,
    count(*)            as num_transactions,
    avg(quantity)       as avg_quantity,
    avg(total_amount)   as avg_amount,
    sum(total_amount)   as total_revenue,
    current_timestamp() as dbt_loaded_at
from `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_p3_transactions`
where transaction_id is not null
group by
    run_date,
    customer_id,
    customer_name,
    customer_city,
    product_category,
    payment_method,
    status