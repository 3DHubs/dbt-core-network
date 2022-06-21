select *
from {{ source('ext_bing', 'ad_groups') }}