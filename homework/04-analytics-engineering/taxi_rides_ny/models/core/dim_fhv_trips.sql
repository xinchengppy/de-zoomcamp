{{
    config(
        materialized='table',
        location='europe-west1' 
    )
}}

with trips_unioned as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.trip_id, 
    trips_unioned.dispatching_base_num,
    trips_unioned.affiliated_base_num, 
    trips_unioned.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.shared_ride_flag, 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_location_id = dropoff_zone.locationid