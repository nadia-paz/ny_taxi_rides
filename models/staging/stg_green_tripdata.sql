{{ config(materialized='view') }}

SELECT 
    -- identifiers
    CAST(vendor_id AS INTEGER) AS vendorid,
    CAST(rate_code AS INTEGER) AS ratecodeid,
    CAST(pickup_location_id AS INTEGER) AS pickup_locationid,
    CAST(dropoff_location_id AS INTEGER) AS dropoff_locationid,

    -- timestamps
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    CAST(trip_type AS INTEGER) AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(imp_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS NUMERIC) AS payment_type,
    {{ get_payment_type_description("payment_type") }} AS payment_type_description


FROM {{ source('staging', 'green_tripdata') }}
limit 100