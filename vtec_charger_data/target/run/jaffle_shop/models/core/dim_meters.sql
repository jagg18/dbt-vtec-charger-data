
  
    

  create  table "postgres-zoomcamp"."vtec_dbt_test"."dim_meters__dbt_tmp"
  
  
    as
  
  (
    

select 
    meter_key, 
    
    
    cast(meter_address as TEXT)
 as meter_address,
    name
from "postgres-zoomcamp"."vtec_dbt_test"."meter_name_lookup"
  );
  