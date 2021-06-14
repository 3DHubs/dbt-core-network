select created,
       updated,
       uuid,
       order_uuid,
       {{ varchar_to_boolean('is_partial') }},
       status,
       ready_for_pickup_at,
       transit_to_warehouse_at,
       at_warehouse_at,
       transit_to_customer_at,
       delivered_at
from int_service_supply.packages