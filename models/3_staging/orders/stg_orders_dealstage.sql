----------------------------------------------------------------
-- DEALSTAGE DATA AT ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Data Lake Hubspot Dealstage History (Closing and Cancellation)
-- 2. Data Lake Supply Order History Events (Completion)

{{ config(
    tags=["multirefresh"]
) }}

with hubspot_dealstage_history as (
    select deal_id,
           min(case when dealstage_mapped like 'Won%' then changed_at end)           as closed_at,
           min(case when dealstage_mapped = 'Closed - Canceled' then changed_at end) as cancelled_at
    from {{ ref('hubspot_deal_dealstage_history') }}
    group by 1
    having (closed_at is not null or cancelled_at is not null) -- Filter to reduce table length

), -- Determining how long a stage spend in new
    hubspot_dealstage_history_new as (
     select deal_id,
            rank() over (partition by deal_id order by changed_at, next_changed_at, primary_key asc ) as rnk,
            changed_at,
            next_changed_at,
            case when office_location = 'Chicago' then  convert_timezone('America/Chicago', changed_at) else
            convert_timezone('Europe/Amsterdam', changed_at) end as changed_at_local,
            case when office_location = 'Chicago' then  convert_timezone('America/Chicago', next_changed_at) else
            convert_timezone('Europe/Amsterdam', next_changed_at) end as next_changed_at_local,
            time_in_stage_minutes as time_in_stage_new_minutes
     from {{ ref('hubspot_deal_dealstage_history') }} deals
        left join {{ ref ('stg_orders_hubspot') }} as stg_hubspot on deals.deal_id = stg_hubspot.hubspot_deal_id
        where dealstage_mapped = 'New'
        and next_dealstage is not null
),
    -- Logic to calculate time spent in DFM for IM orders
    dfm_im_time as (
    select deal_id, 
       next_changed_at as im_deal_sourced_after_dfm_at,
       time_in_stage_minutes as time_in_stage_dfm_minutes,
       rank() over (partition by deal_id order by changed_at, next_changed_at, primary_key asc ) as rnk
       from {{ ref('hubspot_deal_dealstage_history') }}
       where true 
       --and deal_id='11774251862'
       and dealstage_mapped = 'Won - Needs sourcing' 
       and next_dealstage='Won - In production'),


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

       -- Time in new
       hdhn.time_in_stage_new_minutes, 
       office_location,
       coalesce(start_business_hour.business_hour,changed_at_local)  as changed_at_local_business_hour,
       coalesce(end_business_hour.business_hour,next_changed_at_local)  as next_changed_at_local_business_hour,
       {{ business_minutes_between('changed_at_local_business_hour', 'next_changed_at_local_business_hour') }} as time_in_stage_new_business_minutes,
       
       -- DFM time IM
      im_deal_sourced_after_DFM_at,
      time_in_stage_dfm_minutes,

       -- Status
       case when stg_hubspot.hubspot_status_mapped = 'lost' and order_status.mapped_value='canceled'  then stg_hubspot.hubspot_status_mapped
       else
       coalesce(order_status.mapped_value, stg_hubspot.hubspot_status_mapped) end as order_status

from {{ ref ('prep_supply_orders') }} as orders
         left join {{ ref ('prep_supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid
         left join hubspot_dealstage_history as hdh on orders.hubspot_deal_id = hdh.deal_id
         left join hubspot_dealstage_history_new as hdhn on orders.hubspot_deal_id = hdhn.deal_id and hdhn.rnk=1
         left join {{ ref('business_hours') }} start_business_hour on start_business_hour.date_hour = date_trunc('hour',changed_at_local)  and start_business_hour.is_business_hour = false
         left join {{ ref('business_hours') }} end_business_hour on end_business_hour.date_hour = date_trunc('hour',next_changed_at_local)  and end_business_hour.is_business_hour = false
         left join dfm_im_time dit on dit.deal_id = orders.hubspot_deal_id and dit.rnk=1
         left join supply_order_events as soe on orders.uuid = soe.order_uuid
         left join {{ ref ('seed_order_status') }} as order_status on orders.status = order_status.supply_status_value
         left join {{ ref ('stg_orders_hubspot') }} as stg_hubspot on orders.hubspot_deal_id = stg_hubspot.hubspot_deal_id
