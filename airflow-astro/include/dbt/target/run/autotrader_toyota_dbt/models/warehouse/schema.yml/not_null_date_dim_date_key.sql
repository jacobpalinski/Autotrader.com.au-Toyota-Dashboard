select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date_key
from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim`
where date_key is null



      
    ) dbt_internal_test