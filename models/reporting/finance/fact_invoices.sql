-- Fact Invoices

-- This model combines data from the supply DB for old data and unions
-- with Netsuite data for data after March 2021. Invoices with negative sign
-- are credit memos. Invoices are considered recognized based on the recognition date of the order.

with stg_cube_invoices_supply as (
         select invoices.uuid                                                      as invoice_uuid,
                invoices.created                                                   as invoice_created_date,
                invoices.finalized_at                                              as invoice_finalized_at,
                dateadd(day, invoices.payment_term, invoices.finalized_at)         as invoice_due_date,
                invoices.order_uuid                                                as order_uuid,
                invoices.status                                                    as invoice_status,
                invoices.document_number                                           as invoice_document_number,
                invoices.currency_code                                             as invoice_currency_code,
                invoices.subtotal_price_amount / 100.00                            as invoice_subtotal_price_amount,
                round(((invoices.subtotal_price_amount / 100.00) / rates.rate), 2) as invoice_subtotal_price_amount_usd,
                null                                                               as invoice_remaining_amount,
                null                                                               as invoice_remaining_amount_usd,
                null                                                               as order_shipping_revenue_usd,

                -- Recognition Date
                case
                    when orders.recognized_at < current_date and invoices.finalized_at < current_date
                        then true end                                              as invoice_is_recognized,
                case
                    when invoices.finalized_at <= orders.first_completed_at then orders.first_completed_at
                    when invoices.finalized_at > orders.first_completed_at then invoices.finalized_at
                    else null end                                                  as invoice_revenue_date_legacy,
                case
                    when invoice_revenue_date_legacy < '2020-10-01' then invoice_revenue_date_legacy
                    when invoices.finalized_at <= orders.recognized_at then case
                                                                                when orders.recognized_at < '2020-10-01'
                                                                                    then '2020-10-01'
                                                                                else orders.recognized_at end
                    when invoices.finalized_at > orders.recognized_at then case
                                                                               when invoices.finalized_at < '2020-10-01'
                                                                                   then '2020-10-01'
                                                                               else invoices.finalized_at end
                    else null end                                                  as invoice_revenue_date, 

                -- Other Fields
                null                                                               as is_downpayment,
                'supply'                                                           as _data_source

         from {{ ref('cnc_order_quotes') }} as invoices
                left outer join {{ ref('stg_fact_orders') }} as orders using (order_uuid)
                left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                    on rates.currency_code_to = invoices.currency_code and trunc(invoices.finalized_at) = trunc(rates.date)
         where true
           and invoices.type in ('invoice')
           and invoices.finalized_at is not null -- Locked quotes only
           and date_trunc('day', invoices.created)< '2021-03-01'
           ),

-- Netsuite Invoice Data
     stg_cube_invoices_netsuite as (
         select netsuite_trn.internalid::text                    as invoice_uuid,
                netsuite_trn.createddate                         as invoice_created_date,
                netsuite_trn.duedate                             as invoice_due_date,
                invoices.order_uuid                              as order_uuid,
                netsuite_trn.status                              as invoice_status,
                netsuite_trn.tranid                              as invoice_document_number,
                netsuite_trn.currencyname                        as invoice_currency_code,
                -- Credit Memos are negative invoices
                invoice_subtotal_price_amount,
                invoice_subtotal_price_amount_usd,
                invoice_remaining_amount,
                invoice_remaining_amount_usd,
                order_shipping_revenue_usd,
                case
                    when orders.recognized_at < current_date and netsuite_trn.createddate < current_date
                        then true end                            as invoice_is_recognized,
                case
                    when netsuite_trn.createddate <= orders.first_completed_at
                        then orders.first_completed_at
                    when netsuite_trn.createddate > orders.first_completed_at then netsuite_trn.createddate
                    else null end                                as invoice_revenue_date_legacy,
                case
                    when invoice_revenue_date_legacy < '2020-10-01' then invoice_revenue_date_legacy
                    when netsuite_trn.createddate <= orders.recognized_at then case
                                                                                   when orders.recognized_at < '2020-10-01'
                                                                                       then '2020-10-01'
                                                                                   else orders.recognized_at end
                    when netsuite_trn.createddate > orders.recognized_at then case
                                                                                  when netsuite_trn.createddate < '2020-10-01'
                                                                                      then '2020-10-01'
                                                                                  else netsuite_trn.createddate end
                    else null end                                as invoice_revenue_date,
                case when custbody_downpayment > 0 then true end as is_downpayment,
                'netsuite'                                       as _data_source
                
         from {{ ref('netsuite_invoices') }} as netsuite_trn
                left outer join {{ ref('cnc_order_quotes') }} as invoices on invoices.document_number  = netsuite_trn.custbodyquotenumber
                left outer join {{ ref('stg_fact_orders') }} as orders on orders.order_uuid = invoices.order_uuid
         where true
           and date_trunc('day', netsuite_trn.createddate) >= '2021-03-01'
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
       invoice_is_recognized,
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
       invoice_is_recognized,
       invoice_revenue_date_legacy,
       invoice_revenue_date,
       is_downpayment,
       _data_source
from stg_cube_invoices_netsuite