{{ config(materialized='view') }}

SELECT 
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,  
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    -- CAST(vendor_id AS INTEGER) AS vendorid,
    -- CAST(rate_code AS INTEGER) AS ratecodeid,
    -- CAST(pickup_location_id AS INTEGER) AS pickup_locationid,
    -- CAST(dropoff_location_id AS INTEGER) AS dropoff_locationid,

    -- timestamps
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    CAST(trip_distance as NUMERIC) as trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,
    -- CAST(passenger_count AS INTEGER) AS passenger_count,
    -- CAST(trip_distance AS NUMERIC) AS trip_distance,
    -- CAST(trip_type AS INTEGER) AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    -- CAST(payment_type AS NUMERIC) AS payment_type,
    COALESCE({{ dbt.safe_cast("payment_type", api.Column.translate_type("INTEGER")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} AS payment_type_description


FROM {{ source('staging', 'green_tripdata1') }}
WHERE vendorid IS NOT NULL

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'

-- dbt build --select stg_green_tripdata --vars '{'is_test_run': 'false'}'
-- dbt run --select stg_green_tripdata --vars '{'is_test_run': 'false'}'

-- didn't work:
-- dbt build --select stg_green_tripdata --var 'is_test_run: false'
-- dbt run --select stg_green_tripdata --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}