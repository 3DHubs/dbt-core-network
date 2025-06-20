select
order_uuid,
package_uuid,
created,
delivered_at,
estimated_delivery,
tracking_number,
tracking_url,
tracking_carrier_id,
tracking_carrier_name,
shipping_label_id,
provider_label_id,
status,
shipping_leg
from {{ ref('sources_network', 'gold_shipments') }}