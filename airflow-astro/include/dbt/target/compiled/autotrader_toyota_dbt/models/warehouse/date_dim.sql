

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
