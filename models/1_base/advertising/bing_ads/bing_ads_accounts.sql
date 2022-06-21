select *
from {{ source('ext_bing', 'accounts') }}