select *
from ext_netsuite.transaction
where true
    and _type in (
        'Invoice',
        'CreditMemo' -- Negative invoices
        )
    and (not custbody_imported_order or custbody_imported_order is null) -- Excluding manual import of Quickbooks invoices