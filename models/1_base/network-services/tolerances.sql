select id,
       name
from {{ ref('network_services', 'gold_tolerances') }}