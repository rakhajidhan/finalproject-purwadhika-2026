{{ config(materialized='view') }}

select
    *,
    current_timestamp() as loaded_at
from `jcdeah-007.finalproject_rakhajidhan_adakami.prep_adakami_statistics`
