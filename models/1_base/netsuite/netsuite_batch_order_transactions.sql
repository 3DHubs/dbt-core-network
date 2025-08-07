select 
    custbodyquotenumber                         as document_number,
    boolor_agg(custbody_batch_order)          as is_netsuite_batch_order --todo-migration-test

    
from {{ source('ext_netsuite', 'transaction') }} as netsuite_batch_order_transactions
group by 1