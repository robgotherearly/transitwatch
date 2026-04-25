select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select surrogate_key
from "transitwatch"."public_staging"."stg_vehicle_positions"
where surrogate_key is null



      
    ) dbt_internal_test