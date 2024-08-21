select 
    user_id,
    name
from {{ ref('network_services', 'gold_user_roles') }}