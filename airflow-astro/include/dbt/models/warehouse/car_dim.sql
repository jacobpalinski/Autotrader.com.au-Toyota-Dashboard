{{
    config(
        materialized='incremental'
    )
}}

with distinct_cars as (
    select
    distinct
    year,
    car_model,
    type
    from {{ ref('listings_staging') }}
)

select
row_number() over () + (select coalesce(max(car_key), 0) from {{this}}) as car_key,
*
from distinct_cars distinct_cars

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} car_dim
    where car_dim.year = distinct_cars.year
    and car_dim.car_model = distinct_cars.car_model
    and car_dim.type = distinct_cars.type
)
{% endif %}