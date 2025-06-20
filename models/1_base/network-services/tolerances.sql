select id,
       name
from {{ ref('sources_network', 'gold_tolerances') }}