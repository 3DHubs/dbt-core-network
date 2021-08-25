select created,
       updated,
       deleted,
       uuid,
       order_uuid,
       description,
       new_shipping_date,
       reason

from {{ source('int_service_supply', 'order_delays') }}