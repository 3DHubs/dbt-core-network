----------------------------------------------------------------
-- FINANCIAL DATA at ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Stripe Transactions, from Supply.
-- 2. Refunds, from Supply.
-- 3. Docusign Requests.
-- 4. Netsuite, payments.

with stripe_transactions as (
    select t.quote_uuid,
           -- Main Fields
           max(t.created)                                                                     as stripe_transaction_created_at,
           max(case when t.status <> 'failed' and t.type = 'payment' then payment_method end) as stripe_payment_method,
           min(case
                   when (t.status = 'successful' or t.status = 'refunded')
                       and t.type = 'payment'
                       then 1 end)::bool                                                      as stripe_is_successful_payment,
           -- Secondary Fields (Not leveraged but might be useful later)
           sum((fee_amount::float / 100)::decimal(15, 2))                                     as fee_amount,
           min(case when t.status = 'successful' and t.type = 'refund' then 1 end)::bool      as is_successful_refund,
           min(case when t.status = 'failed' and t.type = 'payment' then 1 end)::bool         as is_failed_payment,
           sum(case when t.status = 'failed' and t.type = 'payment' then 1 end)               as num_failed_payments
    from {{ ref('transactions') }} as t
    where status != 'new' -- 'Pending' transactions discarded
    group by 1
), invoice_aggregates as (

select
orders.uuid as order_uuid,
orders.created as order_created_at,
quotes.subtotal_price_amount/100.00 as order_subtotal,
coalesce(count(case  when coalesce(invoices.custbody_downpayment_boolean, false) then order_uuid end),0) as downpayment_invoice_count,

coalesce(sum(case when invoices._type = 'Invoice' then invoices.amountremaining end),0) as invoice_remaining_amount,
coalesce(sum(case when invoices._type = 'CreditMemo' then invoices.unapplied end),0) as credit_remaining_amount,

coalesce(sum(case when invoices._type = 'Invoice' then invoices.amountremaining end),0)
    + coalesce(sum(case when invoices._type = 'CreditMemo' then -invoices.unapplied end),0) as order_remaining_amount,
coalesce(sum(case when invoices._type = 'Invoice' then invoices.invoice_remaining_amount end),0)
    + coalesce(sum(case when invoices._type = 'CreditMemo' then invoices.invoice_remaining_amount end),0) as order_remaining_amount_usd,
coalesce(sum(case when invoices._type = 'Invoice' then invoices.invoice_subtotal_price_amount end),0) as total_invoiced,
coalesce(sum(case when invoices._type = 'CreditMemo' then invoices.invoice_subtotal_price_amount end),0) as total_credited
from {{ ref('prep_supply_orders') }} as orders
left join {{ ref('prep_supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid
left join {{ ref('netsuite_invoices') }} as invoices on invoices.custbodyquotenumber = quotes.document_number
    where true
    group by 1,2,3

), payment_labels as (

    select

invoice_agg.order_uuid,
invoice_agg.order_subtotal,
invoice_agg.downpayment_invoice_count,
invoice_agg.total_invoiced,
invoice_agg.total_credited,
invoice_agg.order_remaining_amount,
invoice_agg.invoice_remaining_amount,
invoice_agg.credit_remaining_amount,
invoice_agg.order_remaining_amount_usd,

case
    when order_created_at  < '2021-03-01' then 'Not Available'
    when coalesce(invoice_agg.total_invoiced,0) = 0 then 'Not yet invoiced'
    
    -- univerisal rules for both downpayment/batch orders
    when invoice_agg.order_remaining_amount = 0 and invoice_agg.total_invoiced = abs(invoice_agg.total_credited)  then 'Fully Refunded'
    when invoice_agg.order_remaining_amount = 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) >= invoice_agg.order_subtotal then 'Fully Paid'
    
    -- rules for order with a remaining amount is negative
    when invoice_agg.order_remaining_amount < 0 then 'Credit on Account / Awaiting Refund'
    
    --- downpayment specific rules
    when invoice_agg.downpayment_invoice_count > 0 then
      case
        when invoice_agg.order_remaining_amount = 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) > 0 and invoice_agg.downpayment_invoice_count = 1 then 'First Downpayment Paid'
        when invoice_agg.order_remaining_amount = 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) < invoice_agg.order_subtotal then 'Issued invoices have been paid'
        when invoice_agg.order_remaining_amount > 0 and invoice_agg.downpayment_invoice_count = 1 then 'First Dowpayment Invoice Awaiting Payment'
        when invoice_agg.order_remaining_amount > 0 then 'Awaiting Payment'
        else 'other'
      end
    
    -- rules for orders that have no remaining amount on account
    when invoice_agg.order_remaining_amount = 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) < invoice_agg.order_subtotal then 'Paid and Partially Refunded/Credit Applied'
    -- rules for orders with a positive remaining amount on account
    when invoice_agg.order_remaining_amount > 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) > invoice_agg.order_remaining_amount then 'Partial Amount Outstanding'
    when invoice_agg.order_remaining_amount > 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) < invoice_agg.order_subtotal then 'Awaiting Payment and Partially Credited'
    when invoice_agg.order_remaining_amount > 0 and (invoice_agg.total_invoiced + invoice_agg.total_credited) >= invoice_agg.order_subtotal  then 'Awaiting Payment'
    else 'other' 

end as payment_label


from invoice_aggregates as invoice_agg

)

select o.uuid                         as order_uuid,
       t.stripe_transaction_created_at,
       t.stripe_is_successful_payment,
       case
           when q.signed_quote_uuid is not null then true
           when q.customer_purchase_order_uuid is not null then true
           when t.stripe_is_successful_payment is true then true
           else false end                is_auto_payment,
       case
           when t.stripe_transaction_created_at::date = dealstage.closed_at::date
               and t.stripe_payment_method <> 'stripe_source_sofort'
               and t.stripe_is_successful_payment is true then true
           else false end             as is_instant_payment,
       case
           when d.envelope_id is not null then '2. Docusign'
           when q.customer_purchase_order_uuid is not null then '3. Purchase order upload'
           when q.signed_quote_uuid is not null then '4. Signed quote upload'
           when t.stripe_payment_method is not null then '1. Stripe payment'
           else '5. Manual Net30' end as payment_method,
        pl.payment_label,
        pl.order_remaining_amount,
        pl.order_remaining_amount_usd
from {{ ref('prep_supply_orders') }} as o
    left join {{ ref('prep_supply_documents') }} as q
    on o.quote_uuid = q.uuid
    left join stripe_transactions as t
    on o.quote_uuid = t.quote_uuid
    left join {{ ref ('quote_docusign_requests') }} as d
    on o.quote_uuid = d.quote_uuid
    left join {{ ref('stg_orders_dealstage') }} as dealstage on o.uuid = dealstage.order_uuid
    left join payment_labels as pl on o.uuid = pl.order_uuid
