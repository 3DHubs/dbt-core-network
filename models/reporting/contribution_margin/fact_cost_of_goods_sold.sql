with tmp_cogs as (
    select poc.po_uuid,
           case
               when orders.order_first_completed_at <= current_date and po_finalized_at <= current_date then date_trunc('day',
                                                                                                             greatest(poc.po_finalized_at, orders.order_first_completed_at))
               else null end   as cost_recognized_date_sept_2020,
           case
               when cost_recognized_date_sept_2020 < '2020-10-01' then cost_recognized_date_sept_2020
               when poc.po_finalized_at <= orders.order_recognised_at then case
                                                                          when orders.order_recognised_at < '2020-10-01'
                                                                              then '2020-10-01'
                                                                          else orders.order_recognised_at end
               when poc.po_finalized_at > orders.order_recognised_at then case
                                                                         when poc.po_finalized_at < '2020-10-01'
                                                                             then '2020-10-01'
                                                                         else poc.po_finalized_at end
               else null end   as cost_recognized_date,
           poc.po_finalized_at as po_date,
           poc.po_currency_code,
           orders.order_technology_id,
           orders.order_technology_name,
           poc.po_order_uuid,
           poc.po_document_number,
           poc.po_subtotal_price_amount,
           poc.po_subtotal_price_amount_usd,
           poc.order_parts_cost,
           poc.order_shipping_cost,
           poc.order_surcharge,
           poc.order_parts_cost_usd,
           poc.order_shipping_cost_usd,
           poc.order_surcharge_usd
    from {{ ref('fact_purchase_orders') }} as poc
             left outer join {{ ref('fact_orders') }} as orders on poc.po_order_uuid = orders.order_uuid
    where true
      and orders.order_recognised_at <= current_date
      and poc.po_finalized_at <= current_date -- Locked PO's only
      -- Order must have at least 1 active PO
      and exists(select 1
                 from {{ ref('fact_purchase_orders') }} poc2
                 where true
                   and poc2.po_order_uuid = poc.po_order_uuid
                   and poc2.po_control_status = 'active')
    ),
stg_lag as (
    select po_uuid,
           lag(po_uuid, 1) over (partition by po_order_uuid order by po_date asc)                      as previous_po_uuid,
           po_order_uuid,
           po_document_number,
           cost_recognized_date,
           po_date,
           po_currency_code,
           order_technology_id,
           order_technology_name,
           po_subtotal_price_amount,
           lag(po_subtotal_price_amount, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_po_subtotal_price_amount,
           po_subtotal_price_amount_usd,
           lag(po_subtotal_price_amount_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_po_subtotal_price_amount_usd,
           order_parts_cost,
           lag(order_parts_cost, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_parts_cost,
           order_shipping_cost,
           lag(order_shipping_cost, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_shipping_cost,
           order_surcharge,
           lag(order_surcharge, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_surcharge,
           order_parts_cost_usd,
           lag(order_parts_cost_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_parts_cost_usd,
           order_shipping_cost_usd,
           lag(order_shipping_cost_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_shipping_cost_usd,
           order_surcharge_usd,
           lag(order_surcharge_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_order_surcharge_usd,
           row_number() over (partition by po_order_uuid order by po_date)                             as rn
    from tmp_cogs
    )
select po_uuid,
        previous_po_uuid,
        po_order_uuid                                                                                           as order_uuid,
        po_document_number,
        cost_recognized_date,
        po_date                                                                                                 as purchase_order_date,
        po_currency_code                                                                                        as source_currency,
        order_technology_id,
        order_technology_name,
        po_subtotal_price_amount_usd -
        (case when rn = 1 then 0 else previous_po_subtotal_price_amount_usd end)                                as cost_usd,
        order_parts_cost_usd -
        (case when rn = 1 then 0 else previous_order_parts_cost_usd end)                                        as cost_parts_usd,
        order_shipping_cost_usd -
        (case when rn = 1 then 0 else previous_order_shipping_cost_usd end)                                     as cost_shipping_usd,
        order_surcharge_usd -
        (case when rn = 1 then 0 else previous_order_surcharge_usd end)                                         as cost_surcharge_usd,
        po_subtotal_price_amount -
        (case when rn = 1 then 0 else previous_po_subtotal_price_amount end)                                    as cost_source_currency,
        order_parts_cost -
        (case when rn = 1 then 0 else previous_order_parts_cost end)                                            as cost_parts_source_currency,
        order_shipping_cost -
        (case when rn = 1 then 0 else previous_order_shipping_cost end)                                         as cost_shipping_source_currency,
        order_surcharge -
        (case when rn = 1 then 0 else previous_order_surcharge end)                                             as cost_surcharge_source_currency
from stg_lag
order by po_uuid, po_date