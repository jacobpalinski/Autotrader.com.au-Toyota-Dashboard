
  
    

    create or replace table `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
    
    

    OPTIONS()
    as (
      with remove_empty_values as (
    select
    *
    from `autotrader-toyota-dashboard`.`autotrader_raw`.`listings_raw`
    where date is not null
    and price is not null
    and price != 'For Auction' 
    and odometer is not null
    and year is not null
    and car_model is not null
    and type is not null
    and suburb is not null
    and state is not null
    and right(odometer, 2) = 'km'
),

uppercase_columns as (
    select 
    date,
    price,
    odometer,
    year,
    upper(car_model) as car_model,
    upper(type) as type,
    upper(suburb) as suburb,
    state
    from remove_empty_values
),

format_columns as (
    select
    date,
    replace(replace(price, '$', ''), ',', '') as price,
    replace(replace(odometer, 'km', ''), ',', '') as odometer,
    year,
    case
    when car_model like '%LAND CRUISER%' then replace(regexp_replace(replace(car_model, type, ''), r'(\S+)\s(.+)', r'\\1\\2'), '\\1\\2', 'LANDCRUISER')
    else replace(car_model, type, '')
    end as car_model,
    case
    when type = '' then 'NOT SPECIFIED'
    else replace(replace(type, '(', ''), ')', '')
    end as type,
    ltrim(replace(replace(suburb, '(', ''), ')', '')) as suburb,
    state
    from uppercase_columns
),

modified_types as (
    select
    parse_date('%Y-%m-%d', date) as date,
    cast(price as INT64) as price,
    cast(odometer as INT64) as odometer,
    cast(year as INT64) as year,
    car_model,
    type,
    suburb,
    state
    from format_columns
)

select distinct * from modified_types
    );
  