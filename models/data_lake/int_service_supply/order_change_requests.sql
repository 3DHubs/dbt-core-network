select *
from {{ source('int_service_supply', 'order_change_requests') }}