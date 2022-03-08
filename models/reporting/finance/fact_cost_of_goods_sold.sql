with tmp_cogs as (
    select poc.po_uuid,
           poc.po_finalized_at as po_date,
           poc.po_currency_code as source_currency,
           poc.po_order_uuid,
           poc.po_document_number,
           -- Recognized Date
           poc.cost_recognized_date,
           -- Financial Amounts
           poc.subtotal_cost,
           poc.parts_cost,
           poc.shipping_cost,
           poc.subtotal_cost_usd,           
           poc.parts_cost_usd,
           poc.shipping_cost_usd
    from {{ ref('fact_purchase_orders') }} as poc
    left join {{ ref('stg_fact_orders') }} as orders on poc.po_order_uuid = orders.order_uuid
    where true
      and orders.recognized_at <= current_date
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
           po_date,
           source_currency,
           po_order_uuid,
           po_document_number,                       
           cost_recognized_date,
           subtotal_cost,           
           lag(subtotal_cost, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_subtotal_cost,
           subtotal_cost_usd,
           lag(subtotal_cost_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_subtotal_cost_usd,
           parts_cost,
           lag(parts_cost, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_parts_cost,
           shipping_cost,
           lag(shipping_cost, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_shipping_cost,
           parts_cost_usd,
           lag(parts_cost_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_parts_cost_usd,
           shipping_cost_usd,
           lag(shipping_cost_usd, 1)
           over (partition by po_order_uuid order by po_date asc)                                      as previous_shipping_cost_usd,
           row_number() over (partition by po_order_uuid order by po_date)                             as rn
    from tmp_cogs
    )
select  po_uuid,
        po_order_uuid                                                                                  as order_uuid,
        po_document_number,
        cost_recognized_date,
        source_currency,
        subtotal_cost_usd -
        (case when rn = 1 then 0 else previous_subtotal_cost_usd end)                          as cost_usd,
        parts_cost_usd -
        (case when rn = 1 then 0 else previous_parts_cost_usd end)                                     as cost_parts_usd,
        shipping_cost_usd -
        (case when rn = 1 then 0 else previous_shipping_cost_usd end)                                  as cost_shipping_usd,
        subtotal_cost -
        (case when rn = 1 then 0 else previous_subtotal_cost end)                              as cost_source_currency,
        parts_cost -
        (case when rn = 1 then 0 else previous_parts_cost end)                                         as cost_parts_source_currency,
        shipping_cost -
        (case when rn = 1 then 0 else previous_shipping_cost end)                                      as cost_shipping_source_currency
from stg_lag