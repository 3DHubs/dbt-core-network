select id,
       value,
       name,
       material_type_id,
       "order",
       {{ varchar_to_boolean('is_default') }}

from {{ source('int_service_supply', 'tolerances') }}