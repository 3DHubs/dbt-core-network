select 
    recognized_date, 
    type, 
    order_uuid, 
    source_document_number,
    amount_usd
from {{ ref('fact_contribution_margin') }} 