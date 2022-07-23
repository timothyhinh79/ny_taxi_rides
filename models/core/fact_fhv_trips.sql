{{ config(materialized='table') }}

SELECT
     fhv_tripdata.tripid
    ,fhv_tripdata.dispatching_base_num
    ,fhv_tripdata.pickup_datetime
    ,fhv_tripdata.pickup_locationID
    ,pickup_zones.borough AS pickup_borough
    ,pickup_zones.zone AS pickup_zone
    ,fhv_tripdata.dropOff_datetime
    ,fhv_tripdata.dropoff_locationID
    ,dropoff_zones.borough AS dropoff_borough
    ,dropoff_zones.zone AS dropoff_zone
    ,fhv_tripdata.shared_ride_flag
    ,fhv_tripdata.affiliated_base_number
FROM {{ref('stg_fhv_tripdata')}} fhv_tripdata
    INNER JOIN {{ref('dim_zones')}} pickup_zones
        ON  fhv_tripdata.pickup_locationID = pickup_zones.locationID
    INNER JOIN {{ref('dim_zones')}} dropoff_zones
        ON  fhv_tripdata.dropoff_locationID = dropoff_zones.locationID