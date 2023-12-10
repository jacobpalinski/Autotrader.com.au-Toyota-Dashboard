{{
    config(
        materialized='incremental'
    )
}}

select
date_dim.date_key as date_key,
car_dim.car_key as car_key,
location_dim.location_key as location_key,
listings_staging.price as price,
listings_staging.odometer as odometer
from {{ ref('listings_staging')}} listings_staging
join {{ref('date_dim')}} date_dim
on listings_staging.date = date_dim.date
join {{ref('car_dim')}} car_dim
on listings_staging.year = car_dim.year
and listings_staging.car_model = car_dim.car_model
and listings_staging.type = car_dim.type
join {{ref('location_dim')}} location_dim
on listings_staging.suburb = location_dim.suburb
and listings_staging.state = location_dim.state

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} listing_fact
    where listing_fact.date_key = date_dim.date_key
    and listing_fact.car_key = car_dim.car_key
    and listing_fact.location_key = location_dim.location_key
)
{% endif %}