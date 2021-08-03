with stg_line_items_netsuite as (
    select custbodyquotenumber as quote_uuid,
            tranid              as netsuite_transaction_id,
            sum(case
                    when item__name = 'Shipping' then nvl(itemlist.amount * quantity, 0)
                    else 0 end) as order_shipping_revenue,
            sum(case
                    when item__name = 'Other revenues' then nvl(itemlist.amount, 0)
                    else 0 end) as order_special_revenue,
            sum(case
                    when item__name in ('3D Printing', 'Injection Molding', 'CNC Revenue',
                                        'Other Techniques', 'Sheet Metal', '3D Printing -')
                        then itemlist.amount * quantity
                    else 0 end) as order_parts_revenue,
            -- Currently not present in Netsuite. Might be added later
            sum(0)              as order_surcharge
    from {{ ref('netsuite_invoices') }} as tran
                left join {{ ref('netsuite_line_items') }} itemlist
                        on tran.internalid = itemlist._sdc_source_key_internalid
    group by 1, 2
),

stg_downpayments_supply as (
    select distinct order_uuid, true as is_downpayment
    from {{ ref('line_items')}} as li
                inner join {{ ref('cnc_order_quotes') }} oqsl on oqsl.uuid = li.quote_uuid
    where lower(title) like '%downpayment%'
),

stg_downpayments_netsuite as (
    select distinct custbodyquotenumber as quote_uuid,
                    tranid              as netsuite_transaction_id,
                    internalid          as netsuite_internal_id,
                    true                as is_downpayment
    from {{ ref ('netsuite_invoices') }}
    where custbody_downpayment > 0
),

stg_cube_invoices_supply as (
    select oqsl.created                                                                     as invoice_created_date,
            oqsl.updated                                                                     as invoice_updated_date,
            oqsl.uuid                                                                        as invoice_uuid,
            oqsl.order_uuid                                                                  as invoice_order_uuid,
            oqsl.revision                                                                    as invoice_revision,
            oqsl.status                                                                      as invoice_status,
            oqsl.finalized_at                                                                as invoice_finalized_at,
            oqsl.document_number                                                             as invoice_document_number,
            oqsl.shipping_address_id                                                         as invoice_shipping_address_id,
            oqsl.payment_reference                                                           as invoice_po_reference,
            oqsl.currency_code                                                               as invoice_currency_code,
            oqsl.price_multiplier                                                            as invoice_price_multiplier,
            oqsl.subtotal_price_amount / 100.00                                              as invoice_subtotal_price_amount,
            round(((oqsl.subtotal_price_amount / 100.00) / rates.rate), 2)                   as invoice_subtotal_price_amount_usd,
            oqsl.tax_price_amount / 100.00                                                   as invoice_tax_price_amount,
            round(((oqsl.tax_price_amount / 100.00) / rates.rate), 2)                        as invoice_tax_price_amount_usd,
            nvl(li.order_parts_revenue, 0) / 100.00                                          as order_parts_revenue,
            nvl(li.order_shipping_revenue, 0) / 100.00                                       as order_shipping_revenue,
            nvl(li.order_surcharge, 0) / 100.00                                              as order_surcharge,
            round(((nvl(li.order_parts_revenue, 0) / 100.00) / rates.rate), 2)               as order_parts_revenue_usd,
            round(((nvl(li.order_shipping_revenue, 0) / 100.00) / rates.rate),
                    2)                                                                         as order_shipping_revenue_usd,
            round(((nvl(li.order_surcharge, 0) / 100.00) / rates.rate), 2)                   as order_surcharge_usd,
            oqsl.tax_rate                                                                    as invoice_tax_rate,
            oqsl.is_instant_payment                                                          as invoice_is_instant_payment,
            oqsl.type                                                                        as quote_type,
            null :: int                                                                      as invoice_supplier_id,
            oqsl.shipping_date                                                               as invoice_shipping_date,
            oqsl.is_admin_only                                                               as invoice_is_admin_only,
            fd.order_quote_uuid,
            fd.order_status,
            fd.delivered_at,
            fd.order_shipped_at,
            fd.order_technology_id,
            fd.order_technology_name,
            case
                when fd.order_recognized_date < current_date and oqsl.finalized_at < current_date then 1
                else 0 end                                                                   as invoice_is_recognized_revenue,
            case
                when oqsl.finalized_at <= fd.order_first_completed_at then fd.order_first_completed_at
                when oqsl.finalized_at > fd.order_first_completed_at then oqsl.finalized_at
                else null end                                                                as invoice_revenue_date_sept_2020,
            case
                when invoice_revenue_date_sept_2020 < '2020-10-01' then invoice_revenue_date_sept_2020
                when oqsl.finalized_at <= fd.order_recognized_date then case
                                                                            when fd.order_recognized_date < '2020-10-01'
                                                                                then '2020-10-01'
                                                                            else fd.order_recognized_date end
                when oqsl.finalized_at > fd.order_recognized_date then case
                                                                            when oqsl.finalized_at < '2020-10-01'
                                                                                then '2020-10-01'
                                                                            else oqsl.finalized_at end
                else null end                                                                as invoice_revenue_date,
            (case when oqsl.finalized_at <= fd.order_recognized_date then 1 else 0 end)::int as is_before_delivery,
            nvl(is_downpayment, false)                                                       as is_downpayment,
            'supply_quotes'                                                                  as _data_source
    from {{ ref('cnc_order_quotes') }} oqsl
                left outer join {{ ref('fact_orders') }} fd using (order_uuid) -- This should be updated to the future `fact_deals` model.
                -- left outer join #stg_line_items_supply as li on li.quote_uuid = oqsl.uuid
                -- Changed this join to rather use the winning_quote (Sales Order) line items from Supply
                -- Yields more complete data
                left outer join {{ ref('agg_quotes_revenue') }} as li on li.quote_uuid = fd.order_quote_uuid
                left outer join stg_downpayments_supply as li_downpayment on li_downpayment.order_uuid = fd.order_uuid
                left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                                on rates.currency_code_to = oqsl.currency_code and
                                trunc(oqsl.finalized_at) = trunc(rates.date)
    where true
        and oqsl.type in ('invoice')
        and oqsl.finalized_at is not null -- Locked quotes only
        and date_trunc('day', oqsl.created) < '2021-03-01'
    order by oqsl.order_uuid, oqsl.created
),

-- Netsuite Invoice Data
stg_cube_invoices_netsuite as (
    select netsuite_trn.createddate                                                                as invoice_created_date,
            netsuite_trn.lastmodifieddate                                                           as invoice_updated_date,
            netsuite_trn.internalid::text                                                           as invoice_uuid,
            fd.order_uuid                                                                           as invoice_order_uuid,
            null::bigint                                                                            as invoice_revision,
            netsuite_trn.status                                                                     as invoice_status,
            null::date                                                                              as invoice_finalized_at,
            netsuite_trn.tranid                                                                     as invoice_document_number,
            -- These don't join to supply_addresses table anymore but to Netsuite addresses table
            netsuite_trn.shippingaddress__internalid::bigint                                        as invoice_shipping_address_id,
            netsuite_trn.custbody_customer_po                                                       as invoice_po_reference,
            netsuite_trn.currencyname                                                               as invoice_currency_code,
            -- Set this to 1 as there is no equivalent in Netsuite
            1                                                                                       as invoice_price_multiplier,
            -- Credit Memos are negative invoices
            case when _type = 'CreditMemo' then -1 * netsuite_trn.subtotal
                                            else netsuite_trn.subtotal end                          as invoice_subtotal_price_amount,
            -- For exchange rates, default null to 1 as this means it is in base USD already. TODO: Confirm this is always true?
            round((invoice_subtotal_price_amount) * nvl(rates.exchangerate, 1.0000),
                    2)                                                                                as invoice_subtotal_price_amount_usd,
            netsuite_trn.taxtotal                                                                   as invoice_tax_price_amount,
            round((netsuite_trn.taxtotal) * nvl(rates.exchangerate, 1.0000),
                    2)                                                                                as invoice_tax_price_amount_usd,
            nvl(li.order_parts_revenue, 0)                                                          as order_parts_revenue,
            nvl(li.order_shipping_revenue, 0)                                                       as order_shipping_revenue,
            nvl(li.order_surcharge, 0)                                                              as order_surcharge,
            nvl(li.order_parts_revenue * nvl(rates.exchangerate, 1.0000), 0)                        as order_parts_revenue_usd,
            nvl(li.order_shipping_revenue * nvl(rates.exchangerate, 1.0000), 0)                     as order_shipping_revenue_usd,
            nvl(li.order_surcharge * nvl(rates.exchangerate, 1.0000), 0)                            as order_surcharge_usd,
            netsuite_trn.taxrate                                                                    as invoice_tax_rate,
            -- Get this from Supply as that is the source towards Netsuite anyway
            fd.is_instant_payment                                                                   as invoice_is_instant_payment,
            netsuite_trn._type                                                                      as quote_type,
            -- Used to get it from the Invoice itself in Supply
            -- For Netsuite we now get it from the PO as we believe this is more accurate
            /*fd.order_active_po_supplier_id*/ fd.supplier_id                                       as invoice_supplier_id,
            netsuite_trn.shipdate                                                                   as invoice_shipping_date,
            -- This concept does not exist in Netsuite anymore
            null::boolean                                                                           as invoice_is_admin_only,
            fd.order_quote_uuid,
            fd.order_status,
            fd.delivered_at,
            fd.order_shipped_at,
            fd.order_technology_id,
            fd.order_technology_name,
            case
                when fd.order_recognized_date < current_date and netsuite_trn.createddate < current_date then 1
                else 0 end                                                                          as invoice_is_recognized_revenue,
            case
                when netsuite_trn.createddate <= fd.order_first_completed_at then fd.order_first_completed_at
                when netsuite_trn.createddate > fd.order_first_completed_at then netsuite_trn.createddate
                else null end                                                                       as invoice_revenue_date_sept_2020,
            case
                when invoice_revenue_date_sept_2020 < '2020-10-01' then invoice_revenue_date_sept_2020
                when netsuite_trn.createddate <= fd.order_recognized_date then case
                                                                                    when fd.order_recognized_date < '2020-10-01'
                                                                                        then '2020-10-01'
                                                                                    else fd.order_recognized_date end
                when netsuite_trn.createddate > fd.order_recognized_date then case
                                                                                    when netsuite_trn.createddate < '2020-10-01'
                                                                                        then '2020-10-01'
                                                                                    else netsuite_trn.createddate end
                else null end                                                                       as invoice_revenue_date,
            (case
                when netsuite_trn.createddate <= fd.order_recognized_date then 1
                else 0 end)::int                                                                   as is_before_delivery,
            nvl(is_downpayment, false)                                                              as is_downpayment,
            'netsuite'                                                                              as _data_source
    from {{ ref('netsuite_invoices') }} as netsuite_trn
                left outer join {{ ref('cnc_order_quotes') }} oqsl2
                                on oqsl2.document_number = netsuite_trn.custbodyquotenumber
                left outer join {{ ref('fact_orders') }} fd on fd.order_uuid = oqsl2.order_uuid -- To do: this dependency has to be changed once new fact_deals model is ready.
                left outer join stg_line_items_netsuite as li on li.netsuite_transaction_id = netsuite_trn.tranid
                left outer join stg_downpayments_netsuite as li_downpayment
                                on li_downpayment.netsuite_transaction_id = netsuite_trn.tranid
                left outer join {{ ref('netsuite_currency_rates') }} as rates
                                on rates.transactioncurrency__internalid = netsuite_trn.currency__internalid
                                    and basecurrency__name = 'USD' and
                                -- Exchange Rates are shifted a day comparing Netsuite vs what lands on RS
                                trunc(netsuite_trn.createddate) = dateadd(day,1,trunc(rates.effectivedate))
    where true
        and date_trunc('day', netsuite_trn.createddate) >= '2021-03-01'
    order by invoice_created_date desc
),

-- Safety Exclusion of data on Supply side
-- If a Sales Order exists on Netsuite side for an order in Supply, don't include the Supply invoice data
stg_netsuite_sales_orders as (
    select distinct invoice_order_uuid
    from stg_cube_invoices_netsuite
)

select invoice_created_date,
        invoice_updated_date,
        invoice_uuid,
        invoice_order_uuid,
        invoice_revision,
        invoice_status,
        invoice_finalized_at,
        invoice_document_number,
        invoice_shipping_address_id,
        invoice_po_reference,
        invoice_currency_code,
        invoice_price_multiplier,
        invoice_subtotal_price_amount,
        invoice_subtotal_price_amount_usd,
        invoice_tax_price_amount,
        invoice_tax_price_amount_usd,
        order_parts_revenue,
        order_shipping_revenue,
        order_surcharge,
        order_parts_revenue_usd,
        order_shipping_revenue_usd,
        order_surcharge_usd,
        invoice_tax_rate,
        invoice_is_instant_payment,
        quote_type,
        invoice_supplier_id,
        invoice_shipping_date,
        invoice_is_admin_only,
        order_quote_uuid,
        order_status,
        delivered_at,
        order_shipped_at,
        order_technology_id,
        order_technology_name,
        invoice_is_recognized_revenue,
        invoice_revenue_date_sept_2020,
        invoice_revenue_date,
        is_before_delivery,
        is_downpayment,
        _data_source
from stg_cube_invoices_supply
union
select invoice_created_date,
        invoice_updated_date,
        invoice_uuid::text                  as invoice_uuid,
        invoice_order_uuid,
        invoice_revision::int               as invoice_revision,
        invoice_status,
        invoice_finalized_at,
        invoice_document_number,
        invoice_shipping_address_id::int    as invoice_shipping_address_id,
        invoice_po_reference,
        invoice_currency_code,
        invoice_price_multiplier,
        invoice_subtotal_price_amount,
        invoice_subtotal_price_amount_usd,
        invoice_tax_price_amount,
        invoice_tax_price_amount_usd,
        order_parts_revenue,
        order_shipping_revenue,
        order_surcharge,
        order_parts_revenue_usd,
        order_shipping_revenue_usd,
        order_surcharge_usd,
        invoice_tax_rate,
        invoice_is_instant_payment::boolean as invoice_is_instant_payment,
        quote_type,
        invoice_supplier_id,
        invoice_shipping_date,
        invoice_is_admin_only,
        order_quote_uuid,
        order_status,
        delivered_at,
        order_shipped_at,
        order_technology_id,
        order_technology_name,
        invoice_is_recognized_revenue,
        invoice_revenue_date_sept_2020,
        invoice_revenue_date,
        is_before_delivery,
        is_downpayment,
        _data_source
from stg_cube_invoices_netsuite