select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        suburb, state, latitude, longitude
    from `autotrader-toyota-dashboard`.`autotrader_staging`.`locations_staging`
    group by suburb, state, latitude, longitude
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test