select
    created,
    order_uuid, 
    anonymous_user_email
from {{ ref('sources_network', 'gold_anonymous_user_carts') }}