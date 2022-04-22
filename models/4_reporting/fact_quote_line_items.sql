-- This model includes only line items from the main quote of an order, it queries from fact_line_items
-- and self joins in order to obtain the costs of the matching line item in the purchase order.

-- Line Items from the Active PO, in some rare cases an order has > 1 active POs.
with active_po_line_items as (
     select order_uuid, 
            line_item_number, 
            line_item_price_amount_usd,
            dense_rank() over (partition by order_uuid order by quote_uuid) as dr 
     from {{ ref('fact_line_items') }} 
     where true
        and is_active_po
        and line_item_type = 'part') 

-- Line Items from the Order's Main Quote (Locked or First)
select qli.*,
       poli.line_item_price_amount_usd as line_item_cost_usd
from (select * from {{ ref('fact_line_items') }} where is_order_quote) as qli
left join (select * from active_po_line_items where dr = 1) as poli -- Eliminates scenario with > 1 active PO, extremely rare.
    on qli.order_uuid = poli.order_uuid and qli.line_item_number = poli.line_item_number -- Note: upload_id and title were not suitable for joining
