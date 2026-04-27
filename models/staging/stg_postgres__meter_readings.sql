{{
    config(
        materialized='view'
    )
}}

with meter_readings as
(
  -- Pull the raw EKM meter records from Postgres before applying any business rules.
  select *
  from {{ source('ekm_data','ekm_meter_data') }}
),

typed_meter_readings as
(
  -- Standardize raw source fields into the data types used by downstream models.
  select
    nullif(trim({{ dbt.safe_cast("meter", api.Column.translate_type("string")) }}), '') as meter_address,
    cast(start_time_stamp as timestamp) as start_date_time,
    cast(end_time_stamp as timestamp) as end_date_time,
    cast(kwh_tot_diff as numeric) as total_usage_kwh
  from meter_readings
),

valid_meter_readings as
(
  -- Remove incomplete or invalid records before they reach reporting models.
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
  -- Keep one record for each meter/start timestamp and make discarded duplicates auditable.
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
    -- Metadata timestamp showing when this cleaned model was last built by dbt.
    cast('{{ run_started_at }}' as timestamp) as dbt_cleaned_at
from deduplicated_meter_readings
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
