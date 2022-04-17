-- This model creates an overview of the Sales Orders created in Netsuite which is the quote equavalent document in Netsuite.
select
nt_so.internalid                                                                    as internal_id,
nt_so.tranid                                                                        as document_number,
nt_so.createddate                                                                   as sales_order_created_at,
quote.order_uuid,
round(nt_so.subtotal,2)                                                             as subtotal_amount,
round((nt_so.subtotal) * nvl(rates.exchangerate, 1.0000),2)                         as subtotal_amount_usd,
nt_so.custbody_fullfilment_status                                                   as fullfilment_status,
nt_so.custbody_fulfillment_date                                                     as fullfilment_at,
case 
    when nt_so.status = 'Pending Approval' then 'Pending Billing'
    when nt_so.status = 'Closed' then 'Cancelled'
    else nt_so.status
end                                                                                 as billing_status,
nt_so.custbody2                                                                     as is_instant_pay,
nt_so.custbody_batch_order                                                          as is_batch_order,
nt_so.custbody_downpayment_boolean                                                  as is_downpayment,

-- Billable Status status indicates if the sales order can be invoiced already or not. 
-- An sales order becomes eligible for invoicing upon order recognition, however, the recognition has to be passed to netsuite through an fulfillment update (recognition date equivalent in netsuite) on the sales order. 
-- Only when the order has become recognized and the fulfillment has update has been received by Netsuite will the the sales order be invoiced.
case 
    when nt_so.status = 'Billed' then 'Billed'
    when nt_so.status != 'Billed' and nt_so.custbody_fulfillment_date is not null and nt_so.custbody_fulfillment_date > '2021-03-01' then 'Eligible for invoicing'
    when nt_so.status != 'Billed' and nt_so.custbody_fulfillment_date is null and fo.recognized_at > '2021-03-01' then 'Eligible for invoicing not fulfilled'
    when nt_so.status != 'Billed' and nt_so.custbody_fulfillment_date is null and fo.recognized_at is null then  'Not fulfilled not Eligible for invoicing'
    
    -- Netsuite is our current book keeping system which was implemented after March 1 2021, therefore, sales order recognized before this period should not be considered.
    when nt_so.status != 'Billed' and fo.recognized_at < '2021-03-01' then 'pre Netsuite'
    else 'Other'
end                                                                                 as  billable_status

from {{ source('ext_netsuite', 'transaction') }} as nt_so
left outer join {{ ref('netsuite_currency_rates') }} as rates
    on rates.transactioncurrency__internalid = nt_so.currency__internalid
    and basecurrency__name = 'USD' and
    trunc(nt_so.createddate) = dateadd(day,1,trunc(rates.effectivedate))
left outer join {{ ref('prep_supply_documents') }} as quote
    on quote.document_number = nt_so.custbodyquotenumber
left outer join {{ ref('fact_orders') }} as fo on fo.order_uuid = quote.order_uuid
where nt_so._type = 'SalesOrder'