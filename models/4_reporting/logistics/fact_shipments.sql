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

{{ config(
    tags=["multirefresh"]
) }}

with fact_after_ship_last_messages as (
    select *
    from {{ ref('fact_aftership_messages') }} as fam
    where fam.is_last_message
),
     fact_after_ship_aggregate_message as (
         select fam.carrier_tracking_number,
                count(*)                                                         as tracking_message_count,

                -- values related to evaluation of shipment validity
                max(case
                        when fam.message_number = 1 then fam.tracking_message_received_at
                        else null end)                                           as first_message_received,
                max(case
                        when fam.message_number = 3 then fam.tracking_message_received_at
                        else null end)                                           as third_message_received,

                -- values related to alerts
                sum(case when fam.has_logistics_message_alert then 1 else 0 end) as number_logistics_message_alerts,
                sum(case when fam.has_tracking_status_alert then 1 else 0 end)   as number_tracking_status_alerts,

                -- dates related to tracking shipment checkpoints such as 'In Transit', 'Delivered'.
                MIN(case
                        when fam.tracking_status = 'In Transit' or fam.tracking_status = 'Acceptance scan' or
                             fam.tracking_status = 'Arrival scan' then fam.tracking_message_received_at
                        else Null end)
                                                                                 as tracking_received_by_carrier_at,
                MAX(case
                        when fam.tracking_status = 'Available for pickup' then fam.tracking_status
                        else Null end)                                           as tracking_available_for_pick_up_at,
                MAX(fam.tracking_latest_expected_delivery)                       as tracking_estimated_delivery,
                MAX(case
                        when fam.tracking_status = 'Delivered' then fam.tracking_message_received_at
                        else Null end)                                           as tracking_delivered_at
         from {{ ref('fact_aftership_messages') }} as fam
         group by 1
     )

select 
    -- Identifiers
    s.order_uuid,
    s.package_uuid                                                                                     as batch_uuid,
    s.tracking_number,

    -- Dates
    s.created                                                                                          as shipment_created_at,
    s.delivered_at                                                                                     as shipment_delivered_at,
    s.estimated_delivery                                                                               as shipment_estimated_delivery_at,

    -- Carrier
    sc.carrier_name_mapped,
    sc.name                                                                                            as carrier_name,

    -- Other Attributes
    s.tracking_url,
    s.status                                                                                           as shipment_status,

    -- Last Tracking Message Updates
    falm.tracking_message_received_at                                                                  as tracking_last_message_received_at,
    falm.tracking_status                                                                               as tracking_last_status,
    falm.tracking_message                                                                              as tracking_last_message,

    -- Platform label indicates if the shipment label was created through the platform
    sl.provider_label_id is not null                                                                   as is_platform_label,

    -- Shipping Leg, Origin, Cross-Docking and Destination
    s.shipping_leg,
    case
        when s.shipping_leg = 'cross_docking:customer' then sog.cross_dock_country
        else sog.origin_country
        end                                                                                            as origin_country,
    case
        when s.shipping_leg = 'cross_docking:warehouse' then sog.cross_dock_country
        else sog.destination_country
        end                                                                                            as destination_country,
    sog.destination_region                                                                             as destination_region,

    -- Alerts:
    -- Logistics alerts: are indications that the shipment might have encountered a delay which can be resolved by logistics these alerts are defined in Fac_aftership_messages.
    falm.has_logistics_message_alert,
    faam.number_logistics_message_alerts,

    -- Tracking alerts: these are alerts defined by aftership which indicates that the shipment might have an delay which might not be resolved by logistics.
    falm.has_tracking_status_alert,
    faam.number_tracking_status_alerts,

    -- Shipment validity:
    -- 99% of shipments which were delivered have at least 3 status updates = messages, 95% of shipments receive the third message within 98 hours of receiving the first.
    -- Therefore if a shipment has not received the 3 message within 98 hours they are considered invalid unless they receive a delivery update.
    case
        when s.status != 'delivered' and s.delivered_at is null and
             faam.third_message_received is null then
                round(DATEDIFF(seconds, coalesce(faam.first_message_received, s.created), getdate()) / 3600.0, 0) < 98
        else True
        end                                                                                            as is_valid_shipment,

    -- Tracking Aggregates
    faam.tracking_received_by_carrier_at,
    faam.tracking_available_for_pick_up_at,
    faam.tracking_estimated_delivery,
    faam.tracking_delivered_at,
    case
        when s.delivered_at is null and faam.tracking_delivered_at is null
            then date_diff('day', tracking_last_message_received_at, current_date)
        else null end                                                                                  as days_since_last_message_update,

    -- If the shipment is the first shipment created for that specific leg, the shipment will be considered for the OTR
    row_number() OVER (PARTITION BY s.order_uuid, s.shipping_leg ORDER BY shipment_created_at asc) =
    1                                                                                                  AS is_first_shipment_of_leg
from {{ source('int_service_supply', 'shipments') }} as s
             left join fact_after_ship_last_messages as falm
on s.tracking_number = falm.carrier_tracking_number
    left join fact_after_ship_aggregate_message as faam on s.tracking_number = faam.carrier_tracking_number
    left join {{ ref('shipping_carriers') }} as sc on s.tracking_carrier_id = sc.id
    left join {{ source('int_service_supply', 'shipping_labels') }} as sl on s.shipping_label_id = sl.id
    left join {{ref('stg_orders_geo')}} as sog on s.order_uuid = sog.order_uuid