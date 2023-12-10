
    
    

with dbt_test__target as (

  select date_key as unique_field
  from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim`
  where date_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


