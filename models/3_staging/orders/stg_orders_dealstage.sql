----------------------------------------------------------------
-- DEALSTAGE DATA AT ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Data Lake Hubspot Dealstage History (Closing and Cancellation)
-- 2. Data Lake Supply Order History Events (Completion)


with hubspot_dealstage_history as (
    select deal_id,
           min(case when dealstage_mapped like 'Won%' then changed_at end)           as closed_at,
           min(case when dealstage_mapped = 'Closed - Canceled' then changed_at end) as cancelled_at
    from {{ ref('hubspot_deal_dealstage_history') }}
    group by 1
    having (closed_at is not null or cancelled_at is not null) -- Filter to reduce table length

),
     -- Only used for completion at the moment (Aug, 2021)
     supply_order_events as (
         select order_uuid,
                min(created) as first_completed_at,
                max(created) as last_completed_at
         from {{ ref('fact_order_events') }}
         where std_event_id = 102
         group by 1
     )

select orders.uuid                                                                         as order_uuid,

       -- Closing
       case when (orders.accepted_at is not null or hdh.closed_at is not null or
                 orders.in_production_at is not null) then true else false end             as is_closed,
       coalesce(orders.accepted_at, orders.in_production_at, hdh.closed_at)                as closed_at,

       -- Cancellation
       hdh.cancelled_at,

       -- Completion
       soe.first_completed_at,
       soe.last_completed_at,

       -- Status
       coalesce(order_status.mapped_value, stg_hubspot.hubspot_status_mapped) as order_status

from {{ ref ('prep_supply_orders') }} as orders
         left join {{ ref ('prep_supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid
         left join hubspot_dealstage_history as hdh on orders.hubspot_deal_id = hdh.deal_id
         left join supply_order_events as soe on orders.uuid = soe.order_uuid
         left join {{ ref ('seed_order_status') }} as order_status on orders.status = order_status.supply_status_value
         left join {{ ref ('stg_orders_hubspot') }} as stg_hubspot
                   on orders.hubspot_deal_id = stg_hubspot.hubspot_deal_id
