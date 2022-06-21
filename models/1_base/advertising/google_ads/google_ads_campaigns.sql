select id, name
from {{ source('ext_google_ads_console', 'campaigns') }}
union
select id, name
from {{ source('ext_adwords', 'campaigns') }}
where id not in (select id
from {{ source('ext_google_ads_console', 'campaigns') }})