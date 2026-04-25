select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select route_id
from "transitwatch"."public_marts"."mart_route_delays"
where route_id is null



      
    ) dbt_internal_test