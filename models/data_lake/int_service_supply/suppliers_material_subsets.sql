select *
from {{ source('int_service_supply', 'suppliers_material_subsets') }}