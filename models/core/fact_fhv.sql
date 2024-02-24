{{
    config(
        materialized='table'
    )
}}

-- Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones. 
-- Similar to what we've done in fact_trips, 
-- keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
-- Run the dbt model without limits (is_test_run: false)

WITH fhv AS (
    SELECT * 
    FROM {{ ref("stg_fhv_tripdata") }}
    -- WHERE pickup_locationid IS NOT NULL AND dropoff_locationid IS NOT NULL
), dz AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT fhv.*, 

  dz_pickup.locationid AS pickup_location,
  dz_pickup.borough AS pickup_borrough,
  dz_pickup.zone AS pickup_zone,
  dz_pickup.service_zone AS pickup_service_zone,

  dz_dropoff.locationid AS dropoff_location,
  dz_dropoff.borough AS dropoff_borrough,
  dz_dropoff.zone AS dropoff_zone,
  dz_dropoff.service_zone AS dropoff_service_zone

FROM fhv
INNER JOIN dz AS dz_pickup ON fhv.pickup_locationid = dz_pickup.locationid
INNER JOIN dz AS dz_dropoff ON fhv.pickup_locationid = dz_dropoff.locationid

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

-- dbt build --select fact_fhv --vars '{'is_test_run': 'false'}'
-- dbt run --select fact_fhv --vars '{'is_test_run': 'false'}'