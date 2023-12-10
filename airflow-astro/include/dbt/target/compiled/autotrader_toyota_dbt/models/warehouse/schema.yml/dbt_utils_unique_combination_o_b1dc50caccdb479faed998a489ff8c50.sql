





with validation_errors as (

    select
        suburb, state, geolocation
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`
    group by suburb, state, geolocation
    having count(*) > 1

)

select *
from validation_errors


