-- back compat for old kwarg name
  
  
        
    

    

    merge into `autotrader-toyota-dashboard`.`autotrader_warehouse`.`listing_fact` as DBT_INTERNAL_DEST
        using (

select
date_dim.date_key as date_key,
car_dim.car_key as car_key,
location_dim.location_key as location_key,
listings_staging.price as price,
listings_staging.odometer as odometer
from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging` listings_staging
join `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim` date_dim
on listings_staging.date = date_dim.date
join `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim` car_dim
on listings_staging.year = car_dim.year
and listings_staging.car_model = car_dim.car_model
and listings_staging.type = car_dim.type
join `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim` location_dim
on listings_staging.suburb = location_dim.suburb
and listings_staging.state = location_dim.state


where not exists (
    select 1
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`listing_fact` listing_fact
    where listing_fact.car_key = car_dim.car_key
    and listing_fact.location_key = location_dim.location_key
    and listing_fact.price = listings_staging.price
    and listing_fact.odometer = listings_staging.odometer
)

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`date_key`, `car_key`, `location_key`, `price`, `odometer`)
    values
        (`date_key`, `car_key`, `location_key`, `price`, `odometer`)


    