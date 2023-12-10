-- back compat for old kwarg name
  
  
        
    

    

    merge into `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim` as DBT_INTERNAL_DEST
        using (

with distinct_cars as (
    select
    distinct
    year,
    car_model,
    type
    from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
)

select
row_number() over () + (select coalesce(max(car_key), 0) from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim`) as car_key,
*
from distinct_cars distinct_cars


where not exists (
    select 1
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`car_dim` car_dim
    where car_dim.year = distinct_cars.year
    and car_dim.car_model = distinct_cars.car_model
    and car_dim.type = distinct_cars.type
)

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`car_key`, `year`, `car_model`, `type`)
    values
        (`car_key`, `year`, `car_model`, `type`)


    