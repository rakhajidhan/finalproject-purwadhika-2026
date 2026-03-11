

  create or replace view `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_adakami_stats`
  OPTIONS()
  as 

select
    *,
    current_timestamp() as loaded_at
from `jcdeah-007.finalproject_rakhajidhan_adakami.prep_adakami_statistics`;

