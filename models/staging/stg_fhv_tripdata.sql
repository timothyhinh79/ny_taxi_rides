{{ config(materialized='view')}}

WITH fhv_tripdata AS (
    SELECT 
         ROW_NUMBER() OVER (PARTITION BY dispatching_base_num, pickup_datetime) as trip_rn
        ,*
    FROM {{ source('staging', 'fhv_tripdata_partitioned')}}
    WHERE dispatching_base_num IS NOT NULL
)

SELECT
     {{dbt_utils.surrogate_key(['dispatching_base_num','pickup_datetime'])}} AS tripid
    ,CAST(dispatching_base_num AS STRING) AS dispatching_base_num
    ,CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime
    ,CAST(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime
    ,CAST(PUlocationID AS INTEGER) AS pickup_locationID
    ,CAST(DOlocationID AS INTEGER) AS dropoff_locationID
    ,CAST(SR_Flag AS INTEGER) AS shared_ride_flag
    ,CAST(Affiliated_base_number AS STRING) AS affiliated_base_number
FROM fhv_tripdata
WHERE trip_rn = 1

{% if var('is_test_run', default=false) %}
    LIMIT 100
{% endif %}
