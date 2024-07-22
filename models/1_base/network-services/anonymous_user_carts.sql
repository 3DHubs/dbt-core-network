select
    created,
    order_uuid, 
    anonymous_user_email
from {{ ref('network_services', 'gold_anonymous_user_carts') }}