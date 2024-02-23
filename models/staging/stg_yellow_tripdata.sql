{{ config(materialized='view') }}
WITH tripdata AS 
(
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_tripdata1') }}
  WHERE vendorid is not null 
)
SELECT 
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,  
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    CAST(trip_distance as NUMERIC) as trip_distance,
    1 AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(0 AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    -- CAST(payment_type AS NUMERIC) AS payment_type,
    COALESCE({{ dbt.safe_cast("payment_type", api.Column.translate_type("INTEGER")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} AS payment_type_description


FROM tripdata
WHERE rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'

-- dbt build --select stg_yellow_tripdata --vars '{'is_test_run': 'false'}'
-- dbt run --select stg_yellow_tripdata --vars '{'is_test_run': 'false'}'

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}