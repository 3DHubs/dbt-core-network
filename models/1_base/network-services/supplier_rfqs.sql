select
created,
updated,
supplier_id,
support_ticket_id,
order_uuid
from {{ ref('sources_network', 'gold_supplier_rfqs') }}