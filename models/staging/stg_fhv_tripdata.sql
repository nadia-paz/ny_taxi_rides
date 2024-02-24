-- Create a staging model for the fhv data, similar to the ones made for yellow and green data. 
-- Add an additional filter for keeping only records with pickup time in year 2019. 
-- Do not add a deduplication step. Run this models without limits (is_test_run: false).



SELECT 
        dispatching_base_num,

        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,

        {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
        {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

        SR_Flag AS share_ride_flag,
        Affiliated_base_number AS affiliated_base_number

FROM {{ source("staging", "fhv_tripdata")}}
WHERE extract(YEAR FROM pickup_datetime) = 2019

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

-- dbt build --select stg_fhv_tripdata --vars '{'is_test_run': 'false'}'
-- dbt run --select stg_fhvreen_tripdata --vars '{'is_test_run': 'false'}'