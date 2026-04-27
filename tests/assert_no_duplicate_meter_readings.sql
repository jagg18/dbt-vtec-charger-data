-- Fails if the cleaned staging model contains duplicate meter/start timestamp records.
select
    meter_address,
    start_date_time,
    count(*) as duplicate_count
from {{ ref('stg_postgres__meter_readings') }}
group by meter_address, start_date_time
having count(*) > 1
