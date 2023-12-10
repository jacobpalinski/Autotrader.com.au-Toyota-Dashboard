





with validation_errors as (

    select
        date_key, date
    from `autotrader-toyota-dashboard`.`autotrader_warehouse`.`date_dim`
    group by date_key, date
    having count(*) > 1

)

select *
from validation_errors


