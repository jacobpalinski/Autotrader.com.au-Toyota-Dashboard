select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select state
from `autotrader-toyota-dashboard`.`autotrader_staging`.`locations_staging`
where state is null



      
    ) dbt_internal_test