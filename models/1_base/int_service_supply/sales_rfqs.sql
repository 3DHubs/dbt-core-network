with 
sales_rfqs as 
    (
    select
        quote_uuid, 
        application, 
        delivered_by, 
        details,
        row_number() over (partition by quote_uuid order by id desc) as rn
    from {{ source('int_service_supply', 'sales_rfqs') }}
    ) 
select
    sr.quote_uuid, 
    sr.application, 
    sr.details,
    sr.delivered_by
from sales_rfqs sr
where sr.rn = 1
