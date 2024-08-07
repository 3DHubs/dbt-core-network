select technology_id,
name,
slug
from {{ ref('network_services', 'gold_technologies') }}