----------------------------------------------------------------
-- Logistics - Fact Batches
----------------------------------------------------------------

-- Table use case summary:
-- The table displays information on the shipment's journey from supplier to customer and if the order required cross-docking.

-- Last updated: May 31, 2022
-- Maintained by: Daniel Salazar

-- Sources:
-- Service Supply
-- Aftership Message (through Fact Aftership Messages)

-- Note: batch_created_at is not 100% not null

{{ config(
    tags=["multirefresh"]
) }}

select
    fs.order_uuid,
    fs.batch_uuid,
    p.created_at                                                          as batch_created_at,
    decode(p.is_partial, 'true', True, 'false', False)                    as is_batch_partial,
    sog.is_cross_docking_ind                                              as is_cross_docking_bool,
    case when not is_batch_partial then p.delivered_at end                as full_delivered_at,
    listagg(case when fs.shipping_leg = 'cross_docking:warehouse' then fs.carrier_name_mapped
    end, ' ')                                                             as carrier_to_crossdock_at,    --todo-migration-test listagg
    listagg(case when is_cross_docking_bool
                 then (case when fs.shipping_leg = 'cross_docking:customer'
                           then fs.carrier_name_mapped end)
            else (fs.carrier_name_mapped)
            end, ' ')                                                     as carrier_to_customer_at,    --todo-migration-test listagg
    sum(case when fs.is_valid_shipment then 1 else 0 end)                 as number_of_shipments,
    min(case
        when fs.shipping_leg = 'cross_docking:warehouse' then fs.shipment_created_at
    end)                                                                  as label_created_to_crossdock_at,
    min(case
        when fs.shipping_leg = 'cross_docking:warehouse' then fs.tracking_received_by_carrier_at
    end)                                                                  as carrier_received_shipment_to_crossdock_at,
    min(case
        when fs.shipping_leg = 'cross_docking:warehouse' then fs.shipment_estimated_delivery_at
    end)                                                                  as provided_estimate_delivery_to_crossdock_at,
    min(case
        when fs.shipping_leg = 'cross_docking:warehouse' then fs.tracking_estimated_delivery
    end)                                                                  as most_recent_estimate_delivery_to_crossdock_at,
    min(case
        when fs.shipping_leg = 'cross_docking:warehouse' then fs.shipment_delivered_at
    end)                                                                  as delivered_to_crossdock_at,

    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then least(fs.shipment_created_at, fs.tracking_received_by_carrier_at)  -- to account for late label creation date timestamps)
            end)
        else min(fs.shipment_created_at)
    end                                                                   as label_created_to_customer_at,
    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then fs.tracking_received_by_carrier_at
            end)
        when is_cross_docking_bool = False then min(case
            when fs.shipping_leg = 'drop_shipping:customer' and o.promised_shipping_date >= '2024-03-01' -- agreed change to switch drop shipping orders over to carrier pick up time as well.
                then fs.tracking_received_by_carrier_at
            else fs.shipment_created_at
        end)
    end                                                                   as carrier_received_shipment_to_customer_at,
    case
        when is_cross_docking_bool then min(case
            when fs.shipping_leg = 'cross_docking:customer'
                then fs.tracking_received_by_carrier_at
        end)
    end                                                                   as shipped_from_cross_dock_at,

    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then fs.tracking_available_for_pick_up_at
            end)
        else max(fs.tracking_available_for_pick_up_at)
    end                                                                   as customer_available_for_pick_up_at,
    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then fs.shipment_estimated_delivery_at
            end)
        else max(fs.shipment_estimated_delivery_at)
    end                                                                   as provided_estimate_delivery_to_customer_at,
    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then fs.tracking_estimated_delivery
            end)
        else max(fs.tracking_estimated_delivery)
    end                                                                   as most_recent_estimate_delivery_to_customer_at,
    case
        when is_cross_docking_bool
            then min(case
                when fs.shipping_leg = 'cross_docking:customer'
                    then fs.shipment_delivered_at
            end)
        else min(fs.shipment_delivered_at)
    end                                                                   as delivered_to_customer_at,
    coalesce(label_created_to_crossdock_at, label_created_to_customer_at) as initial_shipment_created_at,
    coalesce(sum(case when fs.has_logistics_message_alert then 1 else 0 end), 0)
    > 0                                                                   as batch_has_logistics_alert,
    coalesce(sum(fs.number_logistics_message_alerts), 0) > 0              as batch_has_had_logistics_alerts,
    sum(case when fs.is_valid_shipment then 1 else 0 end) > 0             as is_valid_batch
from {{ ref('fact_shipments') }} as fs
    left join {{ ref('packages') }} as p
        on fs.batch_uuid = p.uuid
    left join {{ ref('prep_supply_orders') }} as o on p.order_uuid = o.uuid
    left join {{ ref('stg_orders_geo') }} as sog on fs.order_uuid = sog.order_uuid
group by 1, 2, 3, 4, 5, 6
