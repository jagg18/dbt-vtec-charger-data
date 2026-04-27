{{
    config(
        materialized='view'
    )
}}

with meter_readings as
(
  -- Start with the same raw source used by the main staging model.
  select *
  from {{ source('ekm_data','ekm_meter_data') }}
),

typed_meter_readings as
(
  -- Apply the same type standardization used for accepted records.
  select
    nullif(trim({{ dbt.safe_cast("meter", api.Column.translate_type("string")) }}), '') as meter_address,
    cast(start_time_stamp as timestamp) as start_date_time,
    cast(end_time_stamp as timestamp) as end_date_time,
    cast(kwh_tot_diff as numeric) as total_usage_kwh
  from meter_readings
),

valid_meter_readings as
(
  -- Only valid rows are considered for duplicate detection.
  select *
  from typed_meter_readings
  where meter_address is not null
    and start_date_time is not null
    and end_date_time is not null
    and total_usage_kwh is not null
    and end_date_time >= start_date_time
    and total_usage_kwh >= 0
),

deduplicated_meter_readings as
(
  -- Rows with rn > 1 are duplicate candidates removed from the reporting pipeline.
  select *,
    row_number() over(
      partition by meter_address, start_date_time
      order by end_date_time desc, total_usage_kwh desc
    ) as rn
  from valid_meter_readings
)

select
    meter_address,
    start_date_time,
    end_date_time,
    total_usage_kwh,
    rn,
    -- Metadata timestamp showing when this duplicate audit model was last built by dbt.
    cast('{{ run_started_at }}' as timestamp) as dbt_cleaned_at
from deduplicated_meter_readings
where rn > 1
