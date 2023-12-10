
    
    

with dbt_test__target as (

  select location_key as unique_field
  from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`location_dim`
  where location_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


