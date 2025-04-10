{{
    config(
        materialized='view'
    )
}}

with meter_readings as
(
  select *,
    row_number() over(partition by meter, start_time_stamp) as rn
  from {{ source('ekm_data','ekm_meter_data') }}
  where meter is not null 
)

select
    {{ dbt.safe_cast("meter", api.Column.translate_type("string")) }} as meter_address,
    cast(start_time_stamp as timestamp) as start_date_time,
    cast(end_time_stamp as timestamp) as end_date_time,
    cast(kwh_tot_diff as numeric) as total_usage_kwh,
    rn
from meter_readings
where rn > 1