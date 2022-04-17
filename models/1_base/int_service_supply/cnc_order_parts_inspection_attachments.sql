select *
from {{ source('int_service_supply', 'cnc_order_parts_inspection_attachments') }}