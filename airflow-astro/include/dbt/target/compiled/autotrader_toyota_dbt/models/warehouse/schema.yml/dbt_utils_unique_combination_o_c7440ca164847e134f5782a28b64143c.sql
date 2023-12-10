





with validation_errors as (

    select
        year, car_model, type
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim`
    group by year, car_model, type
    having count(*) > 1

)

select *
from validation_errors


