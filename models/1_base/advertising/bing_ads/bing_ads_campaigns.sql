select *
from {{ source('ext_bing', 'campaigns') }}