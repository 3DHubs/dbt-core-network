select *
from {{ source('int_service_supply', 'cnc_order_quote_attachments') }}