{{ config(materialized='table') }}

select
    {{ dbt.safe_cast("meter_address", api.Column.translate_type("string")) }} as meter_address,
    name
from {{ ref('meter_name_lookup') }}