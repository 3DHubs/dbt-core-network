select *
from {{ source('int_service_supply', 'material_colors') }}