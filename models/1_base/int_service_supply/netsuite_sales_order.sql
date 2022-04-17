select *
from {{ source('int_service_supply', 'netsuite_sales_order') }}