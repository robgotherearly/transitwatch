select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select stop_id
from "transitwatch"."public_staging"."stg_trip_updates"
where stop_id is null



      
    ) dbt_internal_test