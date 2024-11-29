select
uuid,
order_uuid,
created_at,
delivered_at,
is_partial
from {{ ref('network_services', 'gold_packages') }}