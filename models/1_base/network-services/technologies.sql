select technology_id,
name,
slug
from {{ ref('sources_network', 'gold_technologies') }}