{{ config(materialized='table') }}

select
    cast(lpep_pickup_datetime as date) as trip_date,
    VendorID,
    count(*)                as num_trips,
    avg(passenger_count)    as avg_passengers,
    avg(trip_distance)      as avg_distance,
    avg(fare_amount)        as avg_fare,
    avg(total_amount)       as avg_total,
    sum(total_amount)       as total_revenue,
    current_timestamp()     as dbt_loaded_at
from {{ ref('stg_p2_taxi_trips') }}
where lpep_pickup_datetime is not null
group by
    cast(lpep_pickup_datetime as date),
    VendorID
