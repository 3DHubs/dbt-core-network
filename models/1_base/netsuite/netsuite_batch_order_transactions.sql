select 
    custbodyquotenumber                         as document_number,
    bool_or(custbody_batch_order)               as is_netsuite_batch_order

    
from {{ source('ext_netsuite', 'transaction') }} as netsuite_batch_order_transactions
group by 1