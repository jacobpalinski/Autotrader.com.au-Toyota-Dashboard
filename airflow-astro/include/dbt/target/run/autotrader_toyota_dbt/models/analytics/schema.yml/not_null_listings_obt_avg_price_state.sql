select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select avg_price_state
from `autotrader-toyota-dashboard`.`autotrader_analytics`.`listings_obt`
where avg_price_state is null



      
    ) dbt_internal_test