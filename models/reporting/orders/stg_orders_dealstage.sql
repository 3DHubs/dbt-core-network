----------------------------------------------------------------
-- DEALSTAGE DATA AT ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Data Lake Hubspot Dealstage History (Closing and Cancellation)
-- 2. Data Lake Supply Order History Events (Completion)


with hubspot_dealstage_history as (
    select deal_id,
           min(case when dealstage_mapped like 'Won%' then changed_at end)           as order_closed_at,
           min(case when dealstage_mapped = 'Closed - Canceled' then changed_at end) as order_cancelled_at
    from {{ source('data_lake', 'hubspot_deal_dealstage_history') }}
    group by 1
    having (order_closed_at is not null or order_cancelled_at is not null) -- Filter to reduce table length

),
     supply_order_events as (
         select order_uuid,
                min(case when std_event_id = 103 then created end) as order_first_completed_at,
                max(case when std_event_id = 103 then created end) as order_last_completed_at
         from {{ ref ('fact_order_events') }}
         where std_event_id = 103 -- Filter to reduce table length
         group by 1
     )

select orders.uuid                                                                         as order_uuid,

       -- Closing
       case
           when (orders.accepted_at is not null or hdh.order_closed_at is not null or
                 orders.in_production_at is not null) then true end                        as is_closed_won,
       coalesce(orders.accepted_at, orders.in_production_at, hdh.order_closed_at)          as order_closed_at,

       -- Cancellation
       hdh.order_cancelled_at,

       -- Completion
       soe.order_first_completed_at,
       soe.order_last_completed_at,

       -- Status
       case
           when is_closed_won is false then 'lost'
           else coalesce(order_status.mapped_value, stg_hubspot.hubspot_status_mapped) end as order_status

from {{ ref ('cnc_orders') }} as orders
         left join {{ ref ('cnc_order_quotes') }} as quotes on orders.quote_uuid = quotes.uuid
         left join hubspot_dealstage_history as hdh on orders.hubspot_deal_id = hdh.deal_id
         left join supply_order_events as soe on orders.uuid = soe.order_uuid
         left join {{ ref ('order_status') }} as order_status on orders.status = order_status.supply_status_value
         left join {{ ref ('stg_orders_hubspot') }} as stg_hubspot
                   on orders.hubspot_deal_id = stg_hubspot.hubspot_deal_id
where quotes.submitted_at is not null -- Filter to reduce table length
