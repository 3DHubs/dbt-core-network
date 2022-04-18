----------------------------------------------------------------
-- LOGISTICS DATA at ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Data Lake Supply Cross Docking Tracking Details 20200911
-- 2. Data Lake Supply Shipments (+ addresses, countries and shipping carriers)
-- 3. Data Lake Supply Packages

with supply_cdt as (
    select cdtd.order_uuid,
           1                    as is_cross_docking,
           --This row represents the only available date for these orders when they left the MP
           min(cdtd.created)    as cdtd_shipped_at,
           --This is the only shipment for these orders, even though they are x-dock (early implementation phase)
           min(ss.created)      as cdtd_shipped_from_cross_dock_at,
           min(ss.delivered_at) as cdtd_delivered_at
    from {{ source('data_lake', 'supply_cross_docking_tracking_details_20200911') }} as cdtd
             left join {{ source('int_service_supply', 'shipments') }} as ss
    on ss.order_uuid = cdtd.order_uuid
        left join {{ ref('prep_supply_orders') }} as o
        on o.uuid = cdtd.order_uuid
    group by 1
),
     --select first shipment dates for every order
     -- there are a few cases where one package has many shipments created for a single shipping leg.
     --  talking to eng/logistics about why this is happening and trying to figure out if using min makes sense here
     supply_shipments as (
         select order_uuid,
                count(*)                                                                          as no_of_shipments,
                min(shp.created)                                                                  as shipment_shipped_at,
                min(case
                        when shipping_leg in ('drop_shipping:customer', 'cross_docking:customer')
                            then shp.created end)                                                 as shipment_shipped_to_customer_at,
                min(case
                        when shipping_leg in ('drop_shipping:customer', 'cross_docking:customer')
                            then delivered_at end)                                                as shipment_delivered_to_customer_at,
                min(
                        case when shipping_leg = 'cross_docking:warehouse' then shp.created end)  as shipment_shipped_to_crossdock_at,
                min(
                        case when shipping_leg = 'cross_docking:warehouse' then delivered_at end) as shipment_delivered_to_crossdock_at,
                min(case
                        when shipping_leg = 'cross_docking:warehouse'
                            then estimated_delivery end)                                          as estimated_delivery_to_cross_dock,
                min(case
                        when shipping_leg = 'cross_docking:customer' then estimated_delivery
                        when shipping_leg = 'drop_shipping:customer'
                            then estimated_delivery end)                                          as estimated_delivery_to_customer
         from {{ source('int_service_supply', 'shipments') }} shp
             left join {{ source('int_service_supply', 'shipping_carriers') }} car on car.id = shp.tracking_carrier_id
         where order_uuid not in
             (select order_uuid from {{ source('data_lake'
             , 'supply_cross_docking_tracking_details_20200911') }})
         group by 1
     ),

     --select first package dates for every order
     supply_packages as (
         select p.order_uuid,
                count(distinct uuid)                                as no_of_packages,
                min(p.created)                                      as package_create_at,
                min(ready_for_pickup_at)                            as package_ready_for_pickup_at,
                min(transit_to_warehouse_at)                        as package_shipped_to_crossdock_at,
                min(at_warehouse_at)                                as package_delivered_to_crossdock_at,
                min(transit_to_customer_at)                         as package_shipped_to_customer_at,
                min(p.delivered_at)                                 as package_delivered_to_customer_at,
                min(case when not is_partial then delivered_at end) as full_delivered_at,
                max(case when is_partial then 1 else 0 end)         as is_partial
         from {{ ref('packages') }} p
         where p.status <> 'draft'
         group by 1
     )
     --Use shipping dates from shipments as the source of truth
     --Bring in static cross_docking_tracking_details table - static and to form complete historic view of data
     --The base is any order with an active PO - as they should all generate shipping data

select distinct soq.order_uuid                                                                     as order_uuid,

                ------ Main Fields -------
                --------------------------

                coalesce(ss.no_of_shipments, 1)                                                    as number_of_shipments,
                sp.no_of_packages                                                                  as number_of_packages,

                ---- cross docking fields ----
                case when oq.is_cross_docking is true then true else false end                     as is_cross_docking_ind,
                case when is_cross_docking_ind then po_ship_addr.locality else null end            as cross_dock_city,
                case when is_cross_docking_ind then po_ship_coun.alpha2_code else null end         as cross_dock_country,
                case when is_cross_docking_ind then po_ship_addr.lat else null end                 as cross_dock_latitude,
                case when is_cross_docking_ind then po_ship_addr.lon else null end                 as cross_dock_longitude,

                ss.estimated_delivery_to_cross_dock,
                ss.estimated_delivery_to_customer,
                sp.full_delivered_at,

                ---- Verification and Consistency Fields ----
                ---------------------------------------------

                -- Check consecutive delivery dates in shipments
                case
                    when is_cross_docking_ind is false then null
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_crossdock_at > ss.shipment_delivered_to_crossdock_at then false
                    else true end                                                                  as has_shipment_delivered_to_crossdock_date_consecutive,

                -- Delivery dates always depend on courier data so it doesn't make sense to compare them, only compare to when the shipment is created
                case
                    when is_cross_docking_ind is false and
                         ss.shipment_shipped_to_customer_at > ss.shipment_delivered_to_customer_at then false
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_crossdock_at > ss.shipment_delivered_to_customer_at
                        then false
                    else true end                                                                  as has_shipment_delivered_to_customer_date_consecutive,

                -- Main Consistency Field
                case
                    when is_cross_docking_ind is false and
                         shipment_shipped_to_customer_at > shipment_delivered_to_customer_at then false
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_crossdock_at > ss.shipment_delivered_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_crossdock_at > ss.shipment_delivered_to_crossdock_at
                        then false
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_crossdock_at > ss.shipment_shipped_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ss.shipment_delivered_to_crossdock_at > ss.shipment_shipped_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ss.shipment_delivered_to_crossdock_at > ss.shipment_delivered_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ss.shipment_shipped_to_customer_at > ss.shipment_delivered_to_customer_at
                        then false
                    else true end                                                                  as has_consistent_shipping_info,

                ----- Shipping Dates -----
                --------------------------

                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_at
                    when is_cross_docking_ind = false then ss.shipment_shipped_to_customer_at
                    when is_cross_docking_ind then ss.shipment_shipped_to_crossdock_at end         as shipped_at_raw,
                case
                    when date_trunc('day', shipped_at_raw) >= '2019-10-01' then shipped_at_raw end as shipped_date,
                shipped_date as shipped_at, --added to avoid alias conflict in queries below
                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind then ss.shipment_shipped_to_customer_at
                    else shipped_date end                                                            as shipped_to_customer_at,
                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind
                        then ss.shipment_shipped_to_customer_at end                                as shipped_from_cross_dock_at,

                ----- Delivery Dates -----
                --------------------------
                case
                    when cdt.order_uuid is not null then coalesce(cdt.cdtd_delivered_at, o.delivered_at)
                    when has_shipment_delivered_to_customer_date_consecutive
                        then coalesce(ss.shipment_delivered_to_customer_at, o.delivered_at) end    as order_delivered_at,
                order_delivered_at                                                                 as delivered_at, --added to avoid alias conflict in derived_delivered_at

                coalesce(case
                             when order_delivered_at is not null then order_delivered_at
                             when shipped_to_customer_at + interval '7 days' < current_date and
                                  order_delivered_at is null then shipped_to_customer_at + interval '7 days' end,
                         o.completed_at)                                                           as derived_delivered_at,

                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind and has_shipment_delivered_to_crossdock_date_consecutive
                        then ss.shipment_delivered_to_crossdock_at end                             as delivered_to_cross_dock_at,

                case
                    when delivered_to_cross_dock_at is not null then delivered_to_cross_dock_at
                    when is_cross_docking_ind and shipped_date + interval '7 days' < current_date and
                         delivered_to_cross_dock_at is null
                        then shipped_date + interval '7 days' end                                    as derived_delivered_to_cross_dock_at

from {{ ref('prep_supply_documents') }} as soq
    inner join {{ ref('prep_purchase_orders') }} as pos on soq.uuid = pos.uuid
    left join {{ ref('prep_supply_documents') }} as cnc_po on pos.uuid = cnc_po.uuid
    left join {{ ref('addresses') }} as po_ship_addr on cnc_po.shipping_address_id = po_ship_addr.address_id
    left join  {{ ref('prep_countries') }} po_ship_coun on po_ship_addr.country_id = po_ship_coun.country_id
    left join supply_packages as sp on soq.order_uuid = sp.order_uuid
    left join supply_shipments as ss on sp.order_uuid = ss.order_uuid
    left join supply_cdt as cdt on cdt.order_uuid = sp.order_uuid
    left join {{ ref('prep_supply_orders') }} as o on o.uuid = cnc_po.order_uuid
    --todo: is this join necessary?
    left join {{ ref('prep_supply_documents') }} as oq on oq.uuid = o.quote_uuid
where soq.type = 'purchase_order'
  and pos.status = 'active'
