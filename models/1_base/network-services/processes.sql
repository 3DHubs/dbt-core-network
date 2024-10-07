select process_id,
       name,
       about,
       technology_id
from {{ ref('network_services', 'gold_processes') }}