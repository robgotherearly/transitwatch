select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select avg_delay_s
from "transitwatch"."public_marts"."mart_hourly_performance"
where avg_delay_s is null



      
    ) dbt_internal_test