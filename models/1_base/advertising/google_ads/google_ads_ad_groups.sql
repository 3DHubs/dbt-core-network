select id, name
from {{ source('ext_google_ads_console', 'ad_groups') }}
union
select id, name
from {{ source('ext_adwords', 'ad_groups') }}
where id not in (select id
from {{ source('ext_google_ads_console', 'ad_groups') }})