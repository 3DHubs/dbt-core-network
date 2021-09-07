-- One of the features of product is that a customer can request a reorder from the current order
-- with similar features, when this occurs an order will get assigned an reorder_original_order_uuid
-- with a parent-child relationship.

select reorder.order_uuid as reorder_order_uuid,
       reorder.reorder_original_order_uuid as original_order_uuid,
       true as is_reorder,
       original.created_at as original_order_created_at,
       original.lead_time as original_order_lead_time,
       original.amount_usd as original_order_amount_usd,
       original.total_quantity as original_order_quantity
from {{ ref ('stg_fact_orders') }} as reorder
inner join {{ ref ('stg_fact_orders') }} as original on reorder.reorder_original_order_uuid = original.order_uuid
