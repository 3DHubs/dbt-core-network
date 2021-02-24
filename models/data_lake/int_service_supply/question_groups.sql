select created,
       updated,
       deleted,
       id,
       name,
       description,
       decode(is_order_level, 'true', True, 'false', False)     as is_order_level,
       decode(is_quote_level, 'true', True, 'false', False)     as is_quote_level,
       decode(is_line_item_level, 'true', True, 'false', False) as is_line_item_level,
       machine_name
from int_service_supply.question_groups