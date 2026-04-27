{{
    config(
        materialized='table',
    )
}}

with ekm_meter_readings as (
    -- Add a source-system label after staging has standardized, validated, and deduplicated rows.
    select *, 
        'EKM' as meter_type
    from {{ ref('stg_postgres__meter_readings') }}
), 
dim_meters as (
    -- Dimension table translates raw meter identifiers into reporting-friendly names.
    select * from {{ ref('dim_meters') }}
)

select 
    dim_meters.meter_address,
    dim_meters.name as meter_name,
    ekm_meter_readings.meter_type,
    ekm_meter_readings.start_date_time,
    ekm_meter_readings.end_date_time,
    ekm_meter_readings.total_usage_kwh,
    ekm_meter_readings.dbt_cleaned_at,
    -- Metadata timestamp showing when this reporting table was transformed by dbt.
    cast('{{ run_started_at }}' as timestamp) as dbt_transformed_at
from ekm_meter_readings
inner join dim_meters
on ekm_meter_readings.meter_address = dim_meters.meter_address
{% if is_incremental() %}
  -- For incremental runs, only append records newer than the latest loaded interval.
  where end_date_time > (select max(end_date_time) from {{ this }})
{% endif %}
