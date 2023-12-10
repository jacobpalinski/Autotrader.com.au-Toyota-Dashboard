select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      





with validation_errors as (

    select
        year, car_model, type
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim`
    group by year, car_model, type
    having count(*) > 1

)

select *
from validation_errors



      
    ) dbt_internal_test