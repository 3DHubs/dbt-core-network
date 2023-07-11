with exchange_date_max as (select max(date) as max_date
                           from {{ source('data_lake', 'exchange_rate_spot_daily') }})
select date_add('day',1,date) as date,
       currency_code_base,
       currency_code_to,
       is_success,
       is_historical,
       avg(rate) rate
from {{ source('data_lake', 'exchange_rate_spot_daily') }}
where date = (select max_date from exchange_date_max)
group by 1,2,3,4,5
union all
select date,
       currency_code_base,
       currency_code_to,
       is_success,
       is_historical,
       avg(rate) rate
from {{ source('data_lake', 'exchange_rate_spot_daily') }}
group by 1,2,3,4,5
order by date desc
