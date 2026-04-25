select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select hour_of_day
from "transitwatch"."public_marts"."mart_hourly_performance"
where hour_of_day is null



      
    ) dbt_internal_test