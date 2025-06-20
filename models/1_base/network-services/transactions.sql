select 
    quote_uuid,
    created,
    status,
    type,
    payment_method,
    fee_amount
from {{ ref('sources_network', 'gold_transactions') }}