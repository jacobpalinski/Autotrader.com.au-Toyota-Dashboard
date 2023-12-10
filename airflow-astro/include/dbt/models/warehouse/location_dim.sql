{{
    config(
        materialized='incremental'
    )
}}

select
row_number() over () + (select coalesce(max(location_key), 0) from {{this}}) as location_key,
suburb,
state,
st_geogpoint(longitude, latitude) as geolocation
from {{ ref('locations_staging') }} locations_staging

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} location_dim
    where location_dim.suburb = locations_staging.suburb
    and location_dim.state = locations_staging.state
)
{% endif %}