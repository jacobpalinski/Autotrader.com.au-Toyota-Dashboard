{{
    config(
        materialized='incremental'
    )
}}

with distinct_dates as (
    select
    distinct
    date
    from {{ ref('listings_staging') }}
)

select
row_number() over () + (select coalesce(max(date_key), 0) from {{this}}) as date_key,
date
from distinct_dates

{% if is_incremental() %}
where date not in (select date from {{this}})
{% endif %}

