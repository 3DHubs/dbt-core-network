select
id,
package_line_item_id,
batch_shipment_line_item_id
from {{ ref('network_services', 'gold_batch_package_line_items') }}