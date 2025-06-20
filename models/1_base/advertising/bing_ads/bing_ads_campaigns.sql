select id, name, 'network_amer' as source
from {{ source('ext_bing', 'campaigns') }}
union all
select id, name, 'factory' as source
from {{ source('ext_bing_emea', 'campaigns') }}
union all
select id, name, 'network_emea' as source
from {{ source('ext_bing_emea', 'campaigns') }}