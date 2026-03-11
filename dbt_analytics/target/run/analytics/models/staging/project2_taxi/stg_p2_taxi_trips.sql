

  create or replace view `jcdeah-007`.`finalproject_rakhajidhan_datamart`.`stg_p2_taxi_trips`
  OPTIONS()
  as 

select
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge,
    run_date
from `jcdeah-007.finalproject_rakhajidhan_ny_taxi_preparation.green_tripdata`
where run_date is not null;

