select *
from {{ source('int_service_supply', 'branded_materials') }}
