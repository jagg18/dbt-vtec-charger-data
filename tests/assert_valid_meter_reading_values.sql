-- Fails if invalid readings survive into the cleaned staging layer.
select *
from {{ ref('stg_postgres__meter_readings') }}
where end_date_time < start_date_time
   or total_usage_kwh < 0
