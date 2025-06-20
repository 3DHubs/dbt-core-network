select
id,
quote_uuid,
ship_by_date,
batch_number
from {{ ref('sources_network', 'gold_batch_shipments') }}