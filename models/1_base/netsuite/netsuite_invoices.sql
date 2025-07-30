-- This model pre-process the data from Netsuite transactions and delivers it into fact_invoices.
-- The processing that occurs here is adding line items and exchange rate in an intermediate step.

with stg_line_items_netsuite as (
    select custbodyquotenumber as quote_uuid,
           tranid              as netsuite_transaction_id,
           sum(case
                   when item__name = 'Shipping' then nvl(itemlist.amount, 0)
                   else 0 end) as order_shipping_revenue
    from {{ source('ext_netsuite', 'transaction') }} as tran
                left join {{ ref('netsuite_line_items') }} itemlist
    on tran.internalid = itemlist._sdc_source_key_internalid
    group by 1, 2
)

select netsuite_trn.*,
       -- Credit Memos are negative invoices
       case
           when _type = 'CreditMemo' then -1 * netsuite_trn.subtotal
           else netsuite_trn.subtotal 
       end                                                                                as invoice_subtotal_price_amount,
       coalesce(netsuite_trn.amountremaining, 0) + coalesce(netsuite_trn.unapplied, 0)    as invoice_remaining_amount,
       
       -- For exchange rates, default null to 1 as this means it is in base USD already.
       nvl(rates.exchangerate, 1.0000)                                                    as exchange_rate_invoices,
       round((netsuite_trn.total) * nvl(rates.exchangerate, 1.0000),2)                    as invoice_total_price_amount_usd,
       round((invoice_subtotal_price_amount) * nvl(rates.exchangerate, 1.0000),2)         as invoice_subtotal_price_amount_usd,

       round((invoice_remaining_amount) * nvl(rates.exchangerate, 1.0000), 2)             as remaining_amount_usd,
       case
           when _type = 'CreditMemo' then -1 * remaining_amount_usd
           else remaining_amount_usd
       end                                                                                as invoice_remaining_amount_usd,


       nvl(li.order_shipping_revenue * nvl(rates.exchangerate, 1.0000), 0)                as order_shipping_revenue_usd
from {{ source('ext_netsuite', 'transaction') }} as netsuite_trn
left outer join {{ ref('netsuite_currency_rates') }} as rates
on rates.transactioncurrency__internalid = netsuite_trn.currency__internalid
    and basecurrency__name = 'USD' and
    -- Exchange Rates are shifted a day comparing Netsuite vs what lands on RS
    date_trunc('day', cast(netsuite_trn.createddate as date)) = dateadd(day, 1, date_trunc('day', cast(rates.effectivedate as date))) --todo-migration: date_trunc and check why cast is done
    left outer join stg_line_items_netsuite as li on li.netsuite_transaction_id = netsuite_trn.tranid
where true
  and _type in ('Invoice'
    , 'CreditMemo') -- Negative invoices
  and (not custbody_imported_order
   or custbody_imported_order is null) -- Excluding manual import of Quickbooks invoices