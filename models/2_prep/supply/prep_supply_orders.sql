-- This model queries directly from int_service_supply.cnc_orders as conveniently renames it
-- as supply_orders as cnc_orders is not exclusive to the CNC technology. Furthermore in this model
-- by implementing an inner join with line items we filter out empty orders or carts (orders at their earliest stage).

{{ config(  materialized='table',
            tags=["multirefresh"]
    ) }}

select orders.created,
       orders.updated,
       orders.deleted,
       orders.id,
       orders.billing_request_id,
       orders.hubspot_deal_id,
       orders.uuid,
       orders.reorder_original_order_uuid,
       orders.user_id,
       orders.quote_uuid,
       orders.session_id,
       orders.status,
       orders.delivered_at,
       orders.shipped_at,
       orders.expected_shipping_date,
       orders.legacy_order_id,
       orders.completed_at,
       orders.support_ticket_id,
       orders.number,
       orders.cancellation_reason_id,
       orders.in_production_at,
       orders.promised_shipping_date,
       orders.accepted_at,
       orders.description,
       orders.shipped_to_warehouse_at,

       -- Boolean Fields
       {{ varchar_to_boolean('is_admin') }},
       {{ varchar_to_boolean('is_strategic') }},
       {{ varchar_to_boolean('is_automated_shipping_available') }},
       {{ varchar_to_boolean('should_create_auction') }},
       {{ varchar_to_boolean('is_eligible_for_restriction') }}

from {{ source('int_service_supply', 'cnc_orders') }} as orders
        left join {{ ref('prep_supply_integration') }} as pse on orders.uuid = pse.order_uuid 
-- Filter: only orders with line items on the main quote, this removes empty carts.
where exists (
    select 1 from {{ source('int_service_supply', 'line_items') }} as li
    where orders.quote_uuid = li.quote_uuid
)
-- Filters: external orders created through the PAPI integration
and pse.is_test is not true 