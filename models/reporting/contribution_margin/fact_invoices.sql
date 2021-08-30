-- Fact Invoices

-- This model combines data from the supply DB for old data and unions
-- with Netsuite data for data after March 2021. Invoices with negative sign
-- are credit memos. Invoices are considered recognised based on the recognition date of the order.

with stg_line_items_netsuite as (
    select custbodyquotenumber as quote_uuid,
           tranid              as netsuite_transaction_id,
           sum(case
                   when item__name = 'Shipping' then nvl(itemlist.amount * quantity, 0)
                   else 0 end) as order_shipping_revenue
    from {{ ref('netsuite_invoices') }} as tran
                left join {{ ref('netsuite_line_items') }} itemlist
    on tran.internalid = itemlist._sdc_source_key_internalid
    group by 1, 2
),

     stg_line_items_supply as (
         select quote_uuid,
                sum(case
                        when (li.type = 'shipping' or (li.type = 'custom' and lower(title) like '%shipping%')) and
                             not (li.type = 'shipping' and lower(title) like '%refund%') then nvl(price_amount, 0)
                        else 0 end) as order_shipping_revenue
         from {{ ref('line_items') }} as li
            left join {{ ref('cnc_order_quotes') }} soq
         on soq.uuid = li.quote_uuid
         group by 1
     ),

     stg_cube_invoices_supply as (
         select invoices.uuid                                                      as invoice_uuid,
                invoices.created                                                   as invoice_created_date,
                null                                                               as invoice_due_date,
                invoices.order_uuid                                                as order_uuid,
                invoices.status                                                    as invoice_status,
                invoices.document_number                                           as invoice_document_number,
                invoices.currency_code                                             as invoice_currency_code,
                invoices.subtotal_price_amount / 100.00                            as invoice_subtotal_price_amount,
                round(((invoices.subtotal_price_amount / 100.00) / rates.rate), 2) as invoice_subtotal_price_amount_usd,
                null                                                               as invoice_remaining_amount,
                null                                                               as invoice_remaining_amount_usd,
                round(((nvl(sli.order_shipping_revenue, 0) / 100.00) / rates.rate),
                      2)                                                           as order_shipping_revenue_usd,
                case
                    when orders.order_recognised_at < current_date and invoices.finalized_at < current_date
                        then true end                                              as invoice_is_recognised,
                case
                    when invoices.finalized_at <= orders.order_first_completed_at then orders.order_first_completed_at
                    when invoices.finalized_at > orders.order_first_completed_at then invoices.finalized_at
                    else null end                                                  as invoice_revenue_date_legacy,
                case
                    when invoice_revenue_date_legacy < '2020-10-01' then invoice_revenue_date_legacy
                    when invoices.finalized_at <= orders.order_recognised_at then case
                                                                                      when orders.order_recognised_at < '2020-10-01'
                                                                                          then '2020-10-01'
                                                                                      else orders.order_recognised_at end
                    when invoices.finalized_at > orders.order_recognised_at then case
                                                                                     when invoices.finalized_at < '2020-10-01'
                                                                                         then '2020-10-01'
                                                                                     else invoices.finalized_at end
                    else null end                                                  as invoice_revenue_date,
                null                                                               as is_downpayment,
                'supply'                                                           as _data_source
         from {{ ref('cnc_order_quotes') }} as invoices
                left outer join {{ ref('stg_fact_orders') }} as orders using (order_uuid)
                left outer join stg_line_items_supply as sli
         on sli.quote_uuid = orders.order_quote_uuid
             left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
             on rates.currency_code_to = invoices.currency_code and
             trunc(invoices.finalized_at) = trunc(rates.date)
         where true
           and invoices.type in ('invoice')
           and invoices.finalized_at is not null -- Locked quotes only
           and date_trunc('day'
             , invoices.created)
             < '2021-03-01'
     ),

-- Netsuite Invoice Data
     stg_cube_invoices_netsuite as (
         select netsuite_trn.internalid::text                                       as invoice_uuid,
                netsuite_trn.createddate                                            as invoice_created_date,
                netsuite_trn.duedate                                                as invoice_due_date,
                invoices.order_uuid                                                 as order_uuid,
                netsuite_trn.status                                                 as invoice_status,
                netsuite_trn.tranid                                                 as invoice_document_number,
                netsuite_trn.currencyname                                           as invoice_currency_code,
                -- Credit Memos are negative invoices
                case
                    when _type = 'CreditMemo' then -1 * netsuite_trn.subtotal
                    else netsuite_trn.subtotal end                                  as invoice_subtotal_price_amount,
                -- For exchange rates, default null to 1 as this means it is in base USD already.
                round((invoice_subtotal_price_amount) * nvl(rates.exchangerate, 1.0000),
                      2)                                                            as invoice_subtotal_price_amount_usd,
                netsuite_trn.amountremaining                                        as invoice_remaining_amount,
                round((invoice_remaining_amount) * nvl(rates.exchangerate, 1.0000),
                      2)                                                            as invoice_remaining_amount_usd,

                nvl(li.order_shipping_revenue * nvl(rates.exchangerate, 1.0000), 0) as order_shipping_revenue_usd,
                case
                    when orders.order_recognised_at < current_date and netsuite_trn.createddate < current_date
                        then true end                                               as invoice_is_recognised,
                case
                    when netsuite_trn.createddate <= orders.order_first_completed_at
                        then orders.order_first_completed_at
                    when netsuite_trn.createddate > orders.order_first_completed_at then netsuite_trn.createddate
                    else null end                                                   as invoice_revenue_date_legacy,
                case
                    when invoice_revenue_date_legacy < '2020-10-01' then invoice_revenue_date_legacy
                    when netsuite_trn.createddate <= orders.order_recognised_at then case
                                                                                         when orders.order_recognised_at < '2020-10-01'
                                                                                             then '2020-10-01'
                                                                                         else orders.order_recognised_at end
                    when netsuite_trn.createddate > orders.order_recognised_at then case
                                                                                        when netsuite_trn.createddate < '2020-10-01'
                                                                                            then '2020-10-01'
                                                                                        else netsuite_trn.createddate end
                    else null end                                                   as invoice_revenue_date,
                case when custbody_downpayment > 0 then true end                    as is_downpayment,
                'netsuite'                                                          as _data_source
         from {{ ref('netsuite_invoices') }} as netsuite_trn
                left outer join {{ ref('cnc_order_quotes') }} as invoices
         on invoices.document_number = netsuite_trn.custbodyquotenumber
             left outer join {{ ref('stg_fact_orders') }} as orders on orders.order_uuid = invoices.order_uuid
             left outer join stg_line_items_netsuite as li on li.netsuite_transaction_id = netsuite_trn.tranid
             left outer join {{ ref('netsuite_currency_rates') }} as rates
             on rates.transactioncurrency__internalid = netsuite_trn.currency__internalid
             and basecurrency__name = 'USD' and
             -- Exchange Rates are shifted a day comparing Netsuite vs what lands on RS
             trunc(netsuite_trn.createddate) = dateadd(day,1,trunc(rates.effectivedate))
         where true
           and date_trunc('day'
             , netsuite_trn.createddate) >= '2021-03-01'
     )

select invoice_uuid,
       invoice_created_date,
       invoice_due_date,
       order_uuid,
       invoice_status,
       invoice_document_number,
       invoice_currency_code,
       invoice_subtotal_price_amount,
       invoice_subtotal_price_amount_usd,
       invoice_remaining_amount,
       invoice_remaining_amount_usd,
       order_shipping_revenue_usd,
       invoice_is_recognised,
       invoice_revenue_date_legacy,
       invoice_revenue_date,
       is_downpayment,
       _data_source
from stg_cube_invoices_supply
union
select invoice_uuid,
       invoice_created_date,
       invoice_due_date,
       order_uuid,
       invoice_status,
       invoice_document_number,
       invoice_currency_code,
       invoice_subtotal_price_amount,
       invoice_subtotal_price_amount_usd,
       invoice_remaining_amount,
       invoice_remaining_amount_usd,
       order_shipping_revenue_usd,
       invoice_is_recognised,
       invoice_revenue_date_legacy,
       invoice_revenue_date,
       is_downpayment,
       _data_source
from stg_cube_invoices_netsuite
