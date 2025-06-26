select id, name, 'network_amer' as source
from {{ source('ext_bing', 'campaigns') }}
union all
-- todo-migration: the table below is not available yet, add when available
-- select id, name, 'factory' as source
-- from {{ source('_ext_bing_factory', 'campaigns') }}
-- union all
select id, name, 'network_emea' as source
from {{ source('ext_bing_emea', 'campaigns') }}