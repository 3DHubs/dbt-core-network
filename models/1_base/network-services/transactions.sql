select 
    quote_uuid,
    created,
    status,
    type,
    payment_method,
    fee_amount
from {{ ref('network_services', 'gold_transactions') }}