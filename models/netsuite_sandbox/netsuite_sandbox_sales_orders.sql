{{ config(bind=False) }}

select *
from ext_netsuite_sandbox.transaction
where _type = 'SalesOrder'