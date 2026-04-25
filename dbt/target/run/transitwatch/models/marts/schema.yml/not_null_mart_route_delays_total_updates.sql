select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_updates
from "transitwatch"."public_marts"."mart_route_delays"
where total_updates is null



      
    ) dbt_internal_test