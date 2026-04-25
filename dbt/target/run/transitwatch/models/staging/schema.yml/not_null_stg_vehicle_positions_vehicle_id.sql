select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select vehicle_id
from "transitwatch"."public_staging"."stg_vehicle_positions"
where vehicle_id is null



      
    ) dbt_internal_test