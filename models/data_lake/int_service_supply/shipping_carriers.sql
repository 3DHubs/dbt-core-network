select *
from {{ source('int_service_supply', 'shipping_carriers') }}