select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    surrogate_key as unique_field,
    count(*) as n_records

from "transitwatch"."public_staging"."stg_vehicle_positions"
where surrogate_key is not null
group by surrogate_key
having count(*) > 1



      
    ) dbt_internal_test