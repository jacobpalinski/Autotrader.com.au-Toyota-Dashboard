select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select suburb
from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
where suburb is null



      
    ) dbt_internal_test