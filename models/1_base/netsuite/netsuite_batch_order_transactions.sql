select 
    custbodyquotenumber                         as document_number,
    max(custbody_batch_order)::boolean          as is_netsuite_batch_order --todo-migration-test

    
from {{ source('ext_netsuite', 'transaction') }} as netsuite_batch_order_transactions
group by 1