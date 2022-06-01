----------------------------------------------------------------
-- Logistics - Fact Shipments
----------------------------------------------------------------

-- Table use case summary:
-- The table displays all the data on the shipment level such as the date of shipment delivery and destination.

-- Last updated: May 31, 2022
-- Maintained by: Daniel Salazar

-- Sources:
-- Service Supply
-- Aftership Message (through Fact Aftership Messages)

-- fact_after_ship_last_messages this sub query returns the last message from fact_aftership_messages
with fact_after_ship_last_messages as (
    select *
    from {{ ref('fact_aftership_messages') }} as fam
    where fam.is_last_message
),
     fact_after_ship_aggregate_message as (
         select fam.carrier_tracking_number,
                count(*)                                                         as tracking_message_count,
                max(case
                        when fam.message_number = 1 then fam.tracking_message_received_at
                        else null end)                                           as first_message_received,
                max(case
                        when fam.message_number = 3 then fam.tracking_message_received_at
                        else null end)                                           as third_message_received,
                sum(case when fam.has_logistics_message_alert then 1 else 0 end) as number_logistics_message_alerts,
                sum(case when fam.has_tracking_status_alert then 1 else 0 end)   as number_tracking_status_alerts
         from {{ ref('fact_aftership_messages') }} as fam
         group by 1
     )

select s.order_uuid,
       s.package_uuid as batch_uuid,
       sc.carrier_name_mapped,
       sc.name                                    as carrier_name,
       s.tracking_number,
       s.tracking_url,
       s.shipping_leg,
       s.status                                   as shipment_status,
       s.created                                  as shipment_created_at,
       s.delivered_at                             as shipment_delivered_at,
       s.estimated_delivery                       as shipment_estimated_delivery_at,
       falm.tracking_message_received_at          as tracking_last_message_received_at,
       falm.tracking_status                       as tracking_last_status,
       falm.tracking_message                      as tracking_last_message,


       -- Alerts:
       -- Logistics alerts: are indications that the shipment might have encountered a delay which can be resolved by logistics these alerts are defined in Fac_aftership_messages.
       falm.has_logistics_message_alert,
       faam.number_logistics_message_alerts,

       -- Tracking alerts: these are alerts defined by aftership which indicates that the shipment might have an delay which might not be resolved by logistics.
       falm.has_tracking_status_alert,
       faam.number_tracking_status_alerts,

       -- Platform label indicates if the shipment label was created through the platform
       sl.provider_label_id is not null           as is_platform_label,

       -- Shipment validity:
       -- 99% of shipments which were delivered have at least 3 status updates = messages, 95% of shipments receive the third message within 98 hours of receiving the first.
       -- Therefore if a shipment has not received the 3 message within 98 hours they are considered invalid unless they receive a delivery update.
       case
           when falm.tracking_status = 'Info Received' and s.status != 'delivered' and
                date_diff('day', tracking_last_message_received_at, current_date) > 4 and
                faam.third_message_received is null then
                   round(DATEDIFF(seconds, faam.first_message_received, getdate()) / 3600.0, 0) < 98
           when falm.tracking_status is not null or s.status is not null then True
           else false
           end                                    as is_valid_shipment,

       MIN(case
               when fam.tracking_status = 'In Transit' or fam.tracking_status = 'Acceptance scan' or
                    fam.tracking_status = 'Arrival scan' then fam.tracking_message_received_at
               else Null end)                     as tracking_received_by_carrier_at,
       MAX(case
               when fam.tracking_status = 'Available for pickup' then fam.tracking_status
               else Null end)                     as tracking_available_for_pick_up_at,
       MAX(fam.tracking_latest_expected_delivery) as tracking_estimated_delivery,
       MAX(case
               when fam.tracking_status = 'Delivered' then fam.tracking_message_received_at
               else Null end)                     as tracking_delivered_at,
       case
           when s.delivered_at is null and tracking_delivered_at is null
               then date_diff('day', tracking_last_message_received_at, current_date)
           else null end                          as days_since_last_message_update
from {{ source('int_service_supply', 'shipments') }} as s
             left join fact_after_ship_last_messages as falm
on s.tracking_number = falm.carrier_tracking_number
    left join fact_after_ship_aggregate_message as faam on s.tracking_number = faam.carrier_tracking_number
    left join {{ ref('fact_aftership_messages') }} as fam on s.tracking_number = fam.carrier_tracking_number
    left join {{ ref('shipping_carriers') }} as sc on s.tracking_carrier_id = sc.id
    left join {{ source('int_service_supply', 'shipping_labels') }} as sl on s.shipping_label_id = sl.id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20