select *
from {{ source('int_service_supply', 'netsuite_customers') }}