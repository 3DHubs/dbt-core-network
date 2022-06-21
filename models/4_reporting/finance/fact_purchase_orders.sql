{{ config(
    tags=["notmultipledayrefresh"]
) }}
with purchase_orders as (
select 
       -- Purchase Order Attributes
       oqsl.uuid                                                                             as po_uuid,
       oqsl.order_uuid                                                                       as order_uuid,
       oqsl.finalized_at                                                                     as po_finalized_at,
       oqsl.document_number                                                                  as po_document_number,      
       spocl.status                                                                          as po_control_status, -- Used on filter on fact_cost_of_goods_sold
       -- Recognition Date
        case
        when orders.first_completed_at <= current_date and po_finalized_at <= current_date then date_trunc('day',
                                                                                                        greatest(po_finalized_at, orders.first_completed_at))
        else null end   as cost_recognized_at_sept_2020,
        case
            when orders.po_active_uuid is null then null
            when cost_recognized_at_sept_2020 < '2020-10-01' then cost_recognized_at_sept_2020
            when po_finalized_at <= orders.recognized_at then 
                case
                    when orders.recognized_at < '2020-10-01' then '2020-10-01'
                    else orders.recognized_at 
                end
            when po_finalized_at > orders.recognized_at then 
                case
                    when po_finalized_at < '2020-10-01' then '2020-10-01'
                    else po_finalized_at 
                end
        else null end                                                                        as cost_recognized_at,
        case when cost_recognized_at is not null then True else False end                  as cogs_is_recognized,

       -- Price dimensions
       oqsl.currency_code                                                                    as po_currency_code, 
       (oqsl.subtotal_price_amount::float / 100.00)::decimal(15, 2)                          as subtotal_cost,
       round((subtotal_cost / rates.rate),2)                                                 as subtotal_cost_usd,
       ali.parts_amount                                                                      as parts_cost,
       ali.parts_amount_usd                                                                  as parts_cost_usd,
       ali.shipping_amount                                                                   as shipping_cost,
       ali.shipping_amount_usd                                                               as shipping_cost_usd,
       row_number() over (partition by oqsl.order_uuid order by po_finalized_at)               as rn
from {{ ref('prep_supply_documents') }} as oqsl
            left join {{ ref('prep_supply_orders') }} as osl on oqsl.order_uuid = osl.uuid
            left join {{ ref('stg_fact_orders') }} as orders on oqsl.order_uuid = orders.order_uuid
            left join {{ ref('prep_purchase_orders') }} as spocl on spocl.uuid = oqsl.uuid
            left join {{ ref('agg_line_items') }} as ali on oqsl.uuid = ali.quote_uuid
            left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                            on rates.currency_code_to = oqsl.currency_code
                            and case 
                                    -- The proper exchange rate date here before recognition is the recognition date not delivered date.
                                    -- Correction will be from '2022-04-01' onwards but not retroactively.
                                    when orders.recognized_at >= '2022-04-01' and oqsl.created <= orders.recognized_at then trunc(orders.recognized_at)
                                    when oqsl.created <= osl.delivered_at then trunc(osl.delivered_at)
                                    else trunc(oqsl.created) 
                                end = trunc(rates.date)

                                
where oqsl.type in ('purchase_order'))

select  poc.po_uuid,
        poc.order_uuid,
        poc.po_finalized_at as po_date,
        poc.po_document_number,
        -- Recognized Date
        poc.cost_recognized_at,
        poc.cogs_is_recognized,
        -- Financial Amounts
        poc.po_currency_code as source_currency, 

        lag(subtotal_cost, 1) over (partition by poc.order_uuid order by po_date asc)                                      as previous_subtotal_cost,
        poc.subtotal_cost - (case when rn = 1 then 0 else previous_subtotal_cost end)                                         as cost_source_currency,

        lag(poc.parts_cost, 1) over (partition by poc.order_uuid order by po_date asc)                                     as previous_parts_cost,
        poc.parts_cost - (case when rn = 1 then 0 else previous_parts_cost end)                                               as cost_parts_source_currency,
        
        lag(poc.shipping_cost, 1) over (partition by poc.order_uuid order by po_date asc)                                  as previous_shipping_cost,
        poc.shipping_cost - (case when rn = 1 then 0 else previous_shipping_cost end)                                         as cost_shipping_source_currency,
        
        lag(poc.subtotal_cost_usd, 1) over (partition by poc.order_uuid order by po_date asc)                              as previous_subtotal_cost_usd,
        poc.subtotal_cost_usd - (case when rn = 1 then 0 else previous_subtotal_cost_usd end)                                 as cost_usd,

        lag(poc.parts_cost_usd, 1) over (partition by poc.order_uuid order by po_date asc)                                 as previous_parts_cost_usd,
        poc.parts_cost_usd - (case when rn = 1 then 0 else previous_parts_cost_usd end)                                       as cost_parts_usd,
        
        lag(poc.shipping_cost_usd, 1) over (partition by poc.order_uuid order by po_date asc)                              as previous_shipping_cost_usd,
        poc.shipping_cost_usd - (case when rn = 1 then 0 else previous_shipping_cost_usd end)                                 as cost_shipping_usd
from purchase_orders as poc

