

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
