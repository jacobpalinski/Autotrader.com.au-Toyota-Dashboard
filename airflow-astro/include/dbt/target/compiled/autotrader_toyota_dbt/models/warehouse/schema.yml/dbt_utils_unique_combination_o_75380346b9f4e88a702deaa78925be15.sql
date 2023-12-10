





with validation_errors as (

    select
        date_key, car_key, location_key, price, odometer
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`listing_fact`
    group by date_key, car_key, location_key, price, odometer
    having count(*) > 1

)

select *
from validation_errors


