
  
    

    create or replace table `autotrader-toyota-dashboard`.`autotrader_staging`.`locations_staging`
    
    

    OPTIONS()
    as (
      with uppercase_suburbs as (
    select 
    distinct
    upper(suburb) as suburb,
    state 
    from `autotrader-toyota-dashboard`.`autotrader_raw`.`listings_raw`
    where suburb is not null),

check_duplicates_australian_suburbs as (
    select
    suburb,
    state,
    latitude,
    longitude,
    row_number() over (partition by suburb, state) as row_num
    from `autotrader-toyota-dashboard`.`autotrader_raw`.`australian_suburbs`
)

select 
uppercase_suburbs.suburb as suburb,
uppercase_suburbs.state as state,
check_duplicates_australian_suburbs.latitude as latitude,
check_duplicates_australian_suburbs.longitude as longitude
from
uppercase_suburbs
join check_duplicates_australian_suburbs
on uppercase_suburbs.suburb = check_duplicates_australian_suburbs.suburb
and uppercase_suburbs.state = check_duplicates_australian_suburbs.state
where check_duplicates_australian_suburbs.row_num = 1
    );
  