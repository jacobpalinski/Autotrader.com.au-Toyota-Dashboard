





with validation_errors as (

    select
        suburb, state
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`
    group by suburb, state
    having count(*) > 1

)

select *
from validation_errors


