select *
from {{ source('int_service_supply', 'supplier_processes') }}