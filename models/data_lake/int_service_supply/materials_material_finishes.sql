select *
from {{ source('int_service_supply', 'materials_material_finishes') }}