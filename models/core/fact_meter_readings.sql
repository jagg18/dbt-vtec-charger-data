{{
    config(
        materialized='table',
    )
}}

with ekm_meter_readings as (
    select *, 
        'EKM' as meter_type
    from {{ ref('stg_postgres__meter_readings') }}
), 
dim_meters as (
    select * from {{ ref('dim_meters') }}
)

select 
    dim_meters.name as meter_name,
    ekm_meter_readings.meter_type,
    ekm_meter_readings.start_date_time,
    ekm_meter_readings.end_date_time,
    ekm_meter_readings.total_usage_kwh
from ekm_meter_readings
inner join dim_meters
on ekm_meter_readings.meter_address = dim_meters.meter_address
{% if is_incremental() %}
  where end_date_time > (select max(end_date_time) from {{ this }})
{% endif %}