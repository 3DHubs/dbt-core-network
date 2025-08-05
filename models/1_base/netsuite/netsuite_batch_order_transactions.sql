select 
    custbodyquotenumber                         as document_number,
    max(custbody_batch_order)                   as is_netsuite_batch_order -- todo-migration: bool_ors don't work in snowflake

    
from {{ source('ext_netsuite', 'transaction') }} as netsuite_batch_order_transactions
group by 1