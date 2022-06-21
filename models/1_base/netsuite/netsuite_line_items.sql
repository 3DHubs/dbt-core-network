select *
from {{ source('ext_netsuite', 'transaction__itemlist__item') }}