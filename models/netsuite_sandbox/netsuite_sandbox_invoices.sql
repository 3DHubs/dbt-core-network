{{ config(bind=False) }}

select *
from ext_netsuite_sandbox.transaction
where true
    and _type in (
        'Invoice',
        'CreditMemo' -- Negative invoices
        )