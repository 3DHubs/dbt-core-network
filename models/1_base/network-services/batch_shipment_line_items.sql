select
id,
batch_shipment_id,
quantity,
fulfilled_quantity
from {{ ref('network_services', 'gold_batch_shipments_line_items') }}