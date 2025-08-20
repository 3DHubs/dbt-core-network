-- Maintained by: Daniel
-- Last updated: March 2022

-- This model combines data from the supply DB for old data and unions
-- with Netsuite data for data after March 2021. Invoices with negative sign
-- are credit memos. Invoices are considered recognized based on the recognition date of the order.

-- Invoices originating from supply

with stg_cube_invoices_supply as (

        select invoices.uuid                                                                as invoice_uuid,
            invoices.finalized_at                                                           as invoice_created_date, -- invoice finalization is the moment when the invoice gets created in supply
            dateadd(day, invoices.payment_term, invoices.finalized_at)                      as invoice_due_date,
            invoices.order_uuid                                                             as order_uuid,
            invoices.status                                                                 as invoice_status,
            invoices.document_number                                                        as invoice_document_number,
            invoices.currency_code                                                          as invoice_source_currency,
            (1/rates.rate)                                                                  as exchange_rate_invoices,
            round((invoices.tax_price_amount + invoices.subtotal_price_amount)/100.00 ,2)   as invoice_total_price_amount,
            round((invoice_total_price_amount / rates.rate), 2)                             as invoice_total_price_amount_usd,
            invoices.subtotal_price_amount / 100.00                                         as invoice_subtotal_price_amount,
            round(((invoices.subtotal_price_amount / 100.00) / rates.rate), 2)              as invoice_subtotal_price_amount_usd,
            null::decimal(15, 2)                                                            as invoice_remaining_amount,
            null::decimal(15, 2)                                                            as invoice_remaining_amount_usd,
            null::decimal(15, 2)                                                            as order_shipping_revenue_usd,
                            -- Other Fields
            null                                                                            as is_downpayment,
            'supply'                                                                        as _data_source

        from {{ ref('prep_supply_documents') }} as invoices
            left outer join {{ ref('exchange_rate_daily') }} as rates
                on rates.currency_code_to = invoices.currency_code and date_trunc('day', invoices.finalized_at) = date_trunc('day', rates.date) --todo-migration-test
        where true
           and invoices.type in ('invoice')
           and invoices.finalized_at is not null -- Locked quotes only
           and date_trunc('day', invoices.created)< '2021-03-01'
           ),

-- Invoices originating from Netsuite
     stg_cube_invoices_netsuite as (
         select netsuite_trn.internalid::text                    as invoice_uuid,
                netsuite_trn.createddate                         as invoice_created_date,
                netsuite_trn.duedate                             as invoice_due_date,
                invoices.order_uuid                              as order_uuid,
                netsuite_trn.status                              as invoice_status,
                netsuite_trn.tranid                              as invoice_document_number,
                netsuite_trn.currencyname                        as invoice_source_currency,
                netsuite_trn.exchange_rate_invoices,
                -- Credit Memos are negative invoices\
                netsuite_trn.total                               as invoice_total_price_amount,
                netsuite_trn.invoice_total_price_amount_usd,
                netsuite_trn.invoice_subtotal_price_amount,
                netsuite_trn.invoice_subtotal_price_amount_usd,
                netsuite_trn.invoice_remaining_amount,
                netsuite_trn.invoice_remaining_amount_usd,
                netsuite_trn.order_shipping_revenue_usd,
                case when netsuite_trn.custbody_downpayment > 0 then true end as is_downpayment,
                'netsuite'                                       as _data_source

         from {{ ref('netsuite_invoices') }} as netsuite_trn
                left outer join {{ ref('prep_supply_documents') }} as invoices on invoices.document_number  = netsuite_trn.custbodyquotenumber
                left outer join {{ ref('prep_supply_integration') }} as test_orders on test_orders.document_number = netsuite_trn.custbodyquotenumber
         where true
           and date_trunc('day', netsuite_trn.createddate) >= '2021-03-01'
           and test_orders.is_test is not true 
     ),

     stg_invoices_unionized as (
            select *
            from stg_cube_invoices_supply
            union
            select *
            from stg_cube_invoices_netsuite
     )

    -- Union of all invoices and determining of recognition of invoices.
    select  invoices.*,
        round(invoice_subtotal_price_amount_usd, 2)::decimal(15, 2)                                             as revenue_usd,
        round(coalesce(order_shipping_revenue_usd, 0), 2)::decimal(15, 2)                                       as shipping_revenue_usd,
        round(invoice_subtotal_price_amount, 2)::decimal(15, 2)                                                 as revenue_source_currency,
        case
            when invoices.invoice_created_date <= orders.first_completed_at then orders.first_completed_at
            when invoices.invoice_created_date > orders.first_completed_at then invoices.invoice_created_date
            else null end                                                                                       as revenue_recognized_at_legacy,
        coalesce(seed.recognized_at, date_trunc('day',  
            case
                when invoices.invoice_status = 'processing' then null
                when revenue_recognized_at_legacy < '2020-10-01' then revenue_recognized_at_legacy
                when invoices.invoice_created_date <= orders.recognized_at then 
                    case
                        when orders.recognized_at < '2020-10-01' then '2020-10-01'
                        else orders.recognized_at 
                    end
                when invoices.invoice_created_date > orders.recognized_at then 
                    case
                        when invoices.invoice_created_date < '2020-10-01' then '2020-10-01'
                        else invoices.invoice_created_date 
                    end
            else null 
            end
        ))                                                                                                       as revenue_recognized_at,
        case when revenue_recognized_at is not null then True else False end                                     as revenue_is_recognized,
        orders.exchange_rate_at_closing
     from stg_invoices_unionized as invoices
     left outer join {{ ref('stg_fact_orders') }} as orders on orders.order_uuid = invoices.order_uuid
     left join {{ ref('seed_financial_recognition_invoice_exceptions')}} as seed on seed.source_document_number = invoices.invoice_document_number
