select *
from {{ source('int_service_supply', 'netsuite_customer_addresses') }}