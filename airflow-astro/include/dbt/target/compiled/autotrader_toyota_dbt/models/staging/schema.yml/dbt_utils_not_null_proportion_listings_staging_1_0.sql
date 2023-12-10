







with validation as (
  select
    
    sum(case when None is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 1.0 or not_null_proportion > 1
)
select
  *
from validation_errors

