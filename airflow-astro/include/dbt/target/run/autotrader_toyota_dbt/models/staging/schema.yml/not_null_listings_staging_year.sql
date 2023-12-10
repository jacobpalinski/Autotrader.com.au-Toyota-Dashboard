select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select year
from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
where year is null



      
    ) dbt_internal_test