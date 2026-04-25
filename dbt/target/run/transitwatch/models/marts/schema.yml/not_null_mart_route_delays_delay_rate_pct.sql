select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select delay_rate_pct
from "transitwatch"."public_marts"."mart_route_delays"
where delay_rate_pct is null



      
    ) dbt_internal_test