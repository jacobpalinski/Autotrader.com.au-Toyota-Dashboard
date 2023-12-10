select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select car_key
from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim`
where car_key is null



      
    ) dbt_internal_test