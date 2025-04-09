

with meter_readings as
(
  -- This is a temporary table to hold the meter readings)
  select *,
    row_number() over(partition by meter, start_time_stamp) as rn
  from "postgres-zoomcamp"."public"."ekm_meter_data"
  where meter is not null 
)

select
    -- identifiers
    
    
    cast(meter as TEXT)
 as meter_address,
    
    -- timestamps
    cast(start_time_stamp as timestamp) as start_date_time,
    cast(end_time_stamp as timestamp) as end_date_time,
    
    -- meter info
    cast(kwh_tot_diff as numeric) as total_usage_kwh
from meter_readings
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
