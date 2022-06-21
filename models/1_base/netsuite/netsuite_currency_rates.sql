select *
from {{ source('ext_netsuite', 'currencyrate') }}