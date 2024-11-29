select
created,
updated,
supplier_id,
support_ticket_id,
order_uuid
from {{ ref('network_services', 'gold_supplier_rfqs') }}