-- back compat for old kwarg name
  
  
        
    

    

    merge into `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim` as DBT_INTERNAL_DEST
        using (

with distinct_dates as (
    select
    distinct
    date
    from `autotrader-toyota-dashboard`.`autotrader_staging`.`listings_staging`
)

select
row_number() over () + (select coalesce(max(date_key), 0) from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim`) as date_key,
date
from distinct_dates


where date not in (select date from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim`)

        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`date_key`, `date`)
    values
        (`date_key`, `date`)


    