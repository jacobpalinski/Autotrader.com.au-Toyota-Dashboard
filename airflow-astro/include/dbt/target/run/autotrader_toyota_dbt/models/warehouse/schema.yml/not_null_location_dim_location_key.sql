select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select location_key
from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`
where location_key is null



      
    ) dbt_internal_test