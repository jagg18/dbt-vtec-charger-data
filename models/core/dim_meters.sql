{{ config(materialized='table') }}

-- Map raw meter addresses to readable business names from the seed lookup table.
select
    {{ dbt.safe_cast("meter_address", api.Column.translate_type("string")) }} as meter_address,
    name
from {{ ref('meter_name_lookup') }}
