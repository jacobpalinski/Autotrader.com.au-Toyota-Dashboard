select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select latitude
from `autotrader-toyota-dashboard`.`autotrader_staging`.`locations_staging`
where latitude is null



      
    ) dbt_internal_test