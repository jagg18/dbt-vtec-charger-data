{{
    config(
        materialized='view'
    )
}}

with meter_readings as
(
  -- This is a temporary table to hold the meter readings)
  select *,
    row_number() over(partition by meter, start_time_stamp) as rn
  from {{ source('ekm_data','ekm_meter_data') }}
  where meter is not null 
)

select
    -- identifiers
    {{ dbt.safe_cast("meter", api.Column.translate_type("string")) }} as meter_address,
    
    -- timestamps
    cast(start_time_stamp as timestamp) as start_date_time,
    cast(end_time_stamp as timestamp) as end_date_time,
    
    -- meter info
    cast(kwh_tot_diff as numeric) as total_usage_kwh
from meter_readings
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}