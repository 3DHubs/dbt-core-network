-- Prepares line item data for shipping cost
with stg_line_items as (
select quote_uuid,
        max(technology_id)                                                     as technology_id,
        sum(case
                when (type = 'shipping' or (type = 'custom' and lower(title) like '%shipping%')) and
                        not (type = 'shipping' and lower(title) like '%refund%')
                    then nvl(coalesce(price_amount, unit_price_amount::double precision), 0)
                else 0 end)                                                    as order_shipping_cost,
        sum(case
                when type = 'shipping' and lower(title) like '%refund%' then price_amount
                else 0 end)                                                    as order_special_cost,
        sum(case
                when type = 'part' then nvl(quantity, 0) * nvl(unit_price_amount::double precision, 0)
                else 0 end)                                                    as order_parts_cost,
        sum(case when type = 'surcharge' then nvl(price_amount, 0) else 0 end) as order_surcharge
    from {{ ref('line_items') }} as li
    group by 1
)
select oqsl.created                                                                          as po_create_date,
       oqsl.updated                                                                          as po_updated_date,
       oqsl.uuid                                                                             as po_uuid,
       oqsl.order_uuid                                                                       as po_order_uuid,
       oqsl.revision                                                                         as po_revision,
       oqsl.status                                                                           as po_status,
       oqsl.finalized_at                                                                     as po_finalized_at,
       oqsl.document_number                                                                  as po_document_number,
       oqsl.shipping_address_id                                                              as po_shipping_address_id,
       oqsl.billing_address_id                                                               as po_billing_address_id,
       oqsl.payment_reference                                                                as po_payment_reference,
       oqsl.is_instant_payment                                                               as po_is_instant_payment,
       -- Price dimensions
       oqsl.currency_code                                                                    as po_currency_code,
       oqsl.price_multiplier                                                                 as po_price_multiplier,
       (oqsl.subtotal_price_amount::float / 100.00)::decimal(15, 2)                          as po_subtotal_price_amount,
       (oqsl.tax_price_amount::float / 100.00)::decimal(15, 2)                               as invoice_tax_price_amount,
       (li.order_parts_cost::float / 100.00)::decimal(15, 2)                                 as order_parts_cost,
       (li.order_shipping_cost::float / 100.00)::decimal(15, 2)                              as order_shipping_cost,
       (li.order_surcharge::float / 100.00)::decimal(15, 2)                                  as order_surcharge,
       round(((oqsl.subtotal_price_amount::float / 100.00) / rates.rate),
               2)::decimal(15, 2)                                                              as po_subtotal_price_amount_usd,
       round(((oqsl.tax_price_amount::float / 100.00) / rates.rate), 2)::decimal(15, 2)      as invoice_tax_price_amount_usd,
       round(((li.order_parts_cost::float / 100.00) / rates.rate), 2)::decimal(15, 2)        as order_parts_cost_usd,
       round(((li.order_shipping_cost::float / 100.00) / rates.rate), 2)::decimal(15, 2)     as order_shipping_cost_usd,
       round(((li.order_surcharge::float / 100.00) / rates.rate), 2)::decimal(15, 2)         as order_surcharge_usd,
       -- Tax dimensions
       oqsl.tax_rate                                                                         as po_tax_rate,
       (oqsl.tax_price_amount / 100.00)::decimal(15, 2)                                      as po_tax_price_amount,
       -- Other dimensions
       oqsl.type                                                                             as quote_type,
       oqsl.shipping_date                                                                    as po_shipping_date,
       oqsl.is_admin_only                                                                    as po_is_admin_only,
       orders.order_quote_uuid                                                               as order_first_quote_uuid,
       orders.order_status                                                                   as order_status,
       orders.order_shipped_at                                                               as order_shipped_at,
       orders.delivered_at                                                                   as order_delivery_at,
       osl.completed_at                                                                      as order_completed_at,
       -- Tech is 2 (3DP) when it is null as this is old platform and that is the only option that was available there...
       orders.technology_id                                                                  as order_technology_id,
       orders.technology_name                                                                as order_technology_name,
       spocl.status                                                                          as po_control_status,
       spocl.updated                                                                         as po_control_last_updated_date
from {{ ref('cnc_order_quotes') }} as oqsl
            left outer join {{ ref('cnc_orders') }} as osl on oqsl.order_uuid = osl.uuid
            left outer join {{ ref('cnc_order_quotes') }} as oqslb on oqsl.parent_uuid = oqslb.uuid and oqslb.type = 'bid'
            left outer join {{ ref('auctions_rda') }} as al on al.auction_uuid = oqslb.parent_uuid and al.is_latest_order_auction = True
            left outer join {{ ref('stg_fact_orders') }} as orders on oqsl.order_uuid = orders.order_uuid
            left outer join stg_line_items as li on li.quote_uuid = oqsl.uuid
            left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                            on rates.currency_code_to = oqsl.currency_code
                                -- Use these different dates as this is the "date of realization" logic
                                and case when oqsl.created <= osl.delivered_at then trunc(osl.delivered_at)
                                        else trunc(oqsl.created) end = trunc(rates.date)
            left outer join {{ ref('purchase_orders') }} as spocl on spocl.uuid = oqsl.uuid
where oqsl.type in ('purchase_order')