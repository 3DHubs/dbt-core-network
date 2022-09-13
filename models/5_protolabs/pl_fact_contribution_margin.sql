select 
    recognized_date, 
    type, 
    order_uuid, 
    amount_usd
from {{ ref('fact_contribution_margin') }} 