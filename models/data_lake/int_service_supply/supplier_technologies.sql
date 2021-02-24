select supplier_id,
       technology_id,
       decode(allow_orders_with_finishes, 'true', True, 'false', False) as allow_orders_with_finishes,
       decode(allow_strategic_orders, 'true', True, 'false', False) as allow_strategic_orders,
       decode(allow_non_strategic_orders, 'true', True, 'false', False) as allow_non_strategic_orders,
       decode(allow_super_strategic_orders, 'true', True, 'false', False) as allow_super_strategic_orders,
       max_active_orders,
       max_order_amount,
       strategic_orders_priority,
       min_order_amount,
       min_lead_time
from int_service_supply.supplier_technologies