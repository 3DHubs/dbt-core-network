select process_id,
       name,
       about,
       technology_id
from {{ ref('sources_network', 'gold_processes') }}