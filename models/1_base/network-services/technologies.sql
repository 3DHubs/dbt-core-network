select technology_id,
name

from {{ ref('network_services', 'gold_technologies') }}