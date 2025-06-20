select
id,
batch_shipment_id,
quantity,
fulfilled_quantity
from {{ ref('sources_network', 'gold_batch_shipments_line_items') }}