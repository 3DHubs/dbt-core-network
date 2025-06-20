select
uuid,
order_uuid,
created_at,
delivered_at,
is_partial
from {{ ref('sources_network', 'gold_packages') }}