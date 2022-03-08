select oqsl.created                                                                          as po_create_date,
       oqsl.uuid                                                                             as po_uuid,
       oqsl.order_uuid                                                                       as po_order_uuid,
       oqsl.finalized_at                                                                     as po_finalized_at,
       oqsl.document_number                                                                  as po_document_number,
       oqsl.currency_code                                                                    as po_currency_code,       
       spocl.status                                                                          as po_control_status, -- Used on filter on fact_cost_of_goods_sold
       -- Recognition Date
        case
        when orders.first_completed_at <= current_date and po_finalized_at <= current_date then date_trunc('day',
                                                                                                        greatest(po_finalized_at, orders.first_completed_at))
        else null end   as cost_recognized_date_sept_2020,
        case
        when cost_recognized_date_sept_2020 < '2020-10-01' then cost_recognized_date_sept_2020
        when po_finalized_at <= orders.recognized_at then case
                                                                        when orders.recognized_at < '2020-10-01'
                                                                        then '2020-10-01'
                                                                        else orders.recognized_at end
        when po_finalized_at > orders.recognized_at then case
                                                                        when po_finalized_at < '2020-10-01'
                                                                        then '2020-10-01'
                                                                        else po_finalized_at end
        else null end as cost_recognized_date,
       -- Price dimensions
       (oqsl.subtotal_price_amount::float / 100.00)::decimal(15, 2)                          as subtotal_cost,
       ali.parts_amount as parts_cost,
       ali.shipping_amount as shipping_cost,
       round((subtotal_cost / rates.rate),2)                                         as subtotal_cost_usd,
       ali.parts_amount_usd as parts_cost_usd,
       ali.shipping_amount_usd as shipping_cost_usd

from {{ ref('cnc_order_quotes') }} as oqsl
            left join {{ ref('cnc_orders') }} as osl on oqsl.order_uuid = osl.uuid
            left join {{ ref('cnc_order_quotes') }} as oqslb on oqsl.parent_uuid = oqslb.uuid and oqslb.type = 'bid'
            left join {{ ref('stg_fact_orders') }} as orders on oqsl.order_uuid = orders.order_uuid
            left join {{ ref('purchase_orders') }} as spocl on spocl.uuid = oqsl.uuid
            left join {{ ref('agg_line_items') }} as ali on oqsl.uuid = ali.quote_uuid
            left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                            on rates.currency_code_to = oqsl.currency_code
                                -- Use these different dates as this is the "date of realization" logic
                                and case when oqsl.created <= osl.delivered_at then trunc(osl.delivered_at)
                                        else trunc(oqsl.created) end = trunc(rates.date)
where oqsl.type in ('purchase_order')
