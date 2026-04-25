select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select service_date
from "transitwatch"."public_marts"."mart_hourly_performance"
where service_date is null



      
    ) dbt_internal_test