select id,
       value,
       name,
       material_type_id,
       "order",
       decode(is_default, 'false', False, 'true', True) as is_default
from int_service_supply.tolerances