select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        suburb, state, geolocation
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`
    group by suburb, state, geolocation
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test