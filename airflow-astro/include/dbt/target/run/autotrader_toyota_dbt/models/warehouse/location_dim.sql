-- back compat for old kwarg name
  
  
        
    

    

    merge into `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim` as DBT_INTERNAL_DEST
        using (

select
row_number() over () + (select coalesce(max(location_key), 0) from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`) as location_key,
suburb,
state,
st_geogpoint(longitude, latitude) as geolocation
from `autotrader-toyota-dashboard`.`autotrader_staging`.`locations_staging` locations_staging


where not exists (
    select 1
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim` location_dim
    where location_dim.suburb = locations_staging.suburb
    and location_dim.state = locations_staging.state
)

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`location_key`, `suburb`, `state`, `geolocation`)
    values
        (`location_key`, `suburb`, `state`, `geolocation`)


    