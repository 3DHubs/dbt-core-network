select
id,
quote_uuid,
ship_by_date,
batch_number
from {{ ref('network_services', 'gold_batch_shipments') }}