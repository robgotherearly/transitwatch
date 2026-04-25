select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select delay_rank
from "transitwatch"."public_marts"."mart_stop_hotspots"
where delay_rank is null



      
    ) dbt_internal_test