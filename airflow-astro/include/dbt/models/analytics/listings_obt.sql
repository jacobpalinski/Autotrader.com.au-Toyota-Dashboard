with last_three_months_records as (
    select
    listing_fact.date_key as date_key,
    listing_fact.car_key as car_key,
    listing_fact.location_key as location_key,
    date_dim.date as date,
    car_dim.year as year,
    car_dim.car_model as car_model,
    car_dim.type as type,
    location_dim.suburb as suburb,
    location_dim.state as state,
    location_dim.geolocation as geolocation,
    listing_fact.price as price,
    listing_fact.odometer as odometer
    from {{ref('listing_fact')}} listing_fact
    join {{ref('date_dim')}} date_dim
    on listing_fact.date_key = date_dim.date_key
    join {{ref('car_dim')}} car_dim
    on listing_fact.car_key = car_dim.car_key
    join {{ref('location_dim')}} location_dim
    on listing_fact.location_key = location_dim.location_key
    where date_dim.date >= date_add(current_date(), interval -3 month)
),

calc_avg_price_national as (
    select
    year,
    car_model,
    avg(price) as avg_price_national
    from last_three_months_records
    group by year, car_model
),

calc_avg_price_state as (
    select
    state,
    year,
    car_model,
    avg(price) as avg_price_state
    from last_three_months_records
    group by state, year, car_model
)

select 
last_three_months_records.date_key as date_key,
last_three_months_records.car_key as car_key,
last_three_months_records.location_key as location_key,
last_three_months_records.date as date,
last_three_months_records.suburb as suburb,
last_three_months_records.state as state,
last_three_months_records.geolocation as geolocation,
last_three_months_records.year as year,
last_three_months_records.car_model as car_model,
last_three_months_records.type as type,
last_three_months_records.price as price,
last_three_months_records.odometer as odometer,
calc_avg_price_national.avg_price_national as avg_price_national,
calc_avg_price_state.avg_price_state as avg_price_state
from last_three_months_records
join calc_avg_price_national
on last_three_months_records.year = calc_avg_price_national.year
and last_three_months_records.car_model = calc_avg_price_national.car_model
join calc_avg_price_state
on last_three_months_records.year = calc_avg_price_state.year
and last_three_months_records.car_model = calc_avg_price_state.car_model
and last_three_months_records.state = calc_avg_price_state.state


