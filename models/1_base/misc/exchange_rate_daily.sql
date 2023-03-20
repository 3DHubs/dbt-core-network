with exchange_date_max as (select max(date) as max_date
                           from {{ source('data_lake', 'exchange_rate_spot_daily') }})
select id,
       date_add('day',1,date) as date,
       currency_code_base,
       currency_code_to,
       rate,
       is_success,
       is_historical
from {{ source('data_lake', 'exchange_rate_spot_daily') }}
where date = (select max_date from exchange_date_max)
union all
select id,
       date,
       currency_code_base,
       currency_code_to,
       rate,
       is_success,
       is_historical
from {{ source('data_lake', 'exchange_rate_spot_daily') }}

order by date desc