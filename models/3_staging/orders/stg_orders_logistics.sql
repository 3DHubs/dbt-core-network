----------------------------------------------------------------
-- LOGISTICS DATA at ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Data Lake Supply Cross Docking Tracking Details 20200911
-- 2. Data Lake Supply Shipments (+ addresses, countries and shipping carriers)
-- 3. Data Lake Supply batches



with supply_cdt as (
    select cdtd.order_uuid,
           1                                          as is_cross_docking,
           --This row represents the only available date for these orders when they left the MP
           min(cdtd.created)      as cdtd_shipped_at,
           --This is the only shipment for these orders, even though they are x-dock (early implementation phase)
           min(fb.label_created_to_crossdock_at)      as cdtd_shipped_from_cross_dock_at,
           min(fb.delivered_to_customer_at)           as cdtd_delivered_at
    from {{ source('int_analytics', 'supply_cross_docking_tracking_details_20200911') }} as cdtd
             left join {{ ref('fact_batches') }} as fb
    on fb.order_uuid = cdtd.order_uuid
        left join {{ ref('prep_supply_orders') }} as o
        on o.uuid = cdtd.order_uuid
    group by 1
),
     --select first shipment dates for every order
     -- there are a few cases where one batch has many shipments created for a single shipping leg.
     --  talking to eng/logistics about why this is happening and trying to figure out if using min makes sense here
     agg_batches as (
         select fp.order_uuid,
                sum(fp.number_of_shipments) as number_of_shipments,
                sum(case when fp.is_valid_batch then 1 else 0 end) as number_of_batches,

                min(fp.label_created_to_crossdock_at) as shipment_shipped_to_crossdock_at,
                min(fp.carrier_received_shipment_to_crossdock_at) as carrier_received_shipment_to_crossdock_at,
                min(fp.provided_estimate_delivery_to_crossdock_at) as estimated_delivery_to_cross_dock_at,
                min(fp.delivered_to_crossdock_at) as shipment_delivered_to_crossdock_at,
                
                min(fp.label_created_to_customer_at) as shipment_shipped_to_customer_at,
                min(fp.carrier_received_shipment_to_customer_at) as carrier_received_shipment_to_customer_at,
                min(fp.provided_estimate_delivery_to_customer_at) as estimated_delivery_to_customer_at,
                min(fp.delivered_to_customer_at) as shipment_delivered_to_customer_at,

                min(fp.full_delivered_at) as full_delivered_at
         from {{ ref('fact_batches') }} as fp
         where fp.order_uuid not in
             (select order_uuid from {{ source('int_analytics'
             , 'supply_cross_docking_tracking_details_20200911') }})
         group by 1
     ), 
        -- Select the carrier's used for each shipping leg and which are related to the dates from agg_batches and OTR.
        -- Example Supplier OTR relates to the first shipment to warehouse or to customer (dropshipped) which relates to
        -- the first_leg_carrier_name mentioned below.
        agg_shipment as (
            select fs_first_leg.order_uuid,
                listagg(fs_first_leg.carrier_name, ' ')         as first_leg_carrier_name, --todo-migration-test listagg
                listagg(fs_first_leg.carrier_name_mapped, ' ')  as first_leg_carrier_name_mapped, --todo-migration-test listagg
                listagg(fs_second_leg.carrier_name, ' ')        as second_leg_carrier_name, --todo-migration-test listagg
                listagg(fs_second_leg.carrier_name_mapped, ' ') as second_leg_carrier_name_mapped --todo-migration-test listagg

            from {{ ref('fact_shipments') }} as fs_first_leg
                    left join {{ ref('fact_shipments') }} as fs_second_leg
                            on fs_first_leg.order_uuid = fs_second_leg.order_uuid
                                and fs_second_leg.shipping_leg = 'cross_docking:customer'
                                and fs_second_leg.is_first_shipment_of_leg
            where fs_first_leg.shipping_leg in ('cross_docking:warehouse', 'drop_shipping:customer')
            and fs_first_leg.is_first_shipment_of_leg
            group by 1
            )

        -- Daniel Salazar 2022-08-04:
        -- To do replace agg_batches with agg_shipment using is_first_shipment_of_leg 
        ---using is_first_shipment_of_leg you get the same result as min(fp.label_created_to_crossdock_at))


select distinct soq.order_uuid                                                                     as order_uuid,

                ------ Main Fields -------
                --------------------------

                coalesce(ab.number_of_shipments, 1)                                                as number_of_shipments,
                ab.number_of_batches                                                              as number_of_batches,

                ---- shipment leg fields ---
                agg_s.first_leg_carrier_name,
                agg_s.first_leg_carrier_name_mapped,
                agg_s.second_leg_carrier_name,
                agg_s.second_leg_carrier_name_mapped,

                ---- cross docking fields ----
                sog.is_cross_docking_ind,
                sog.cross_dock_city,
                sog.cross_dock_country,
                sog.cross_dock_latitude,
                sog.cross_dock_longitude,

                sog.origin_country, --to include buffers in stg_orders_otr

                ab.estimated_delivery_to_cross_dock_at,
                ab.estimated_delivery_to_customer_at,
                ab.full_delivered_at,

                ---- Verification and Consistency Fields ----
                ---------------------------------------------

                -- Check consecutive delivery dates in shipments
                --todo-migration-test all following three fields, changed is false = false
                case
                    when is_cross_docking_ind = false then null
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_crossdock_at > ab.shipment_delivered_to_crossdock_at then false
                    else true end                                                                                           as has_shipment_delivered_to_crossdock_date_consecutive,

                -- Delivery dates always depend on courier data so it doesn't make sense to compare them, only compare to when the shipment is created
                case
                    when is_cross_docking_ind = false and
                         ab.carrier_received_shipment_to_customer_at > ab.shipment_delivered_to_customer_at then false
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_crossdock_at > ab.shipment_delivered_to_customer_at
                        then false
                    else true 
                end                                                                                                          as has_shipment_delivered_to_customer_date_consecutive,

                -- Main Consistency Field
                case
                    when is_cross_docking_ind = false and
                         carrier_received_shipment_to_customer_at > shipment_delivered_to_customer_at then false
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_crossdock_at > ab.shipment_delivered_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_crossdock_at > ab.shipment_delivered_to_crossdock_at
                        then false
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_crossdock_at > ab.carrier_received_shipment_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ab.shipment_delivered_to_crossdock_at > ab.carrier_received_shipment_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ab.shipment_delivered_to_crossdock_at > ab.shipment_delivered_to_customer_at
                        then false
                    when is_cross_docking_ind and
                         ab.carrier_received_shipment_to_customer_at > ab.shipment_delivered_to_customer_at
                        then false
                    else true end                                                                  as has_consistent_shipping_info,

                ----- Shipping Dates -----
                --------------------------

                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_at
                    when is_cross_docking_ind = false then ab.shipment_shipped_to_customer_at
                    when is_cross_docking_ind then ab.shipment_shipped_to_crossdock_at end         as shipped_at_label_created_raw,
                case
                    when date_trunc('day', shipped_at_label_created_raw) >= '2019-10-01' then shipped_at_label_created_raw end as shipment_label_created_at,
                
                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_at
                    when is_cross_docking_ind = false then ab.carrier_received_shipment_to_customer_at
                    when is_cross_docking_ind then ab.carrier_received_shipment_to_crossdock_at
                end                                                                                 as shipped_at_raw,
                case when shipped_at_raw::date >= '2019-10-01' then shipped_at_raw end as shipped_at,
                
                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind then ab.carrier_received_shipment_to_customer_at
                    else shipped_at_raw 
                end                                                                                  as shipped_to_customer_at,

                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind
                        then ab.carrier_received_shipment_to_customer_at end                                as shipped_from_cross_dock_at,

                ----- Delivery Dates -----
                --------------------------
                case
                    when cdt.order_uuid is not null then coalesce(cdt.cdtd_delivered_at, o.delivered_at)
                    when has_shipment_delivered_to_customer_date_consecutive then coalesce(ab.shipment_delivered_to_customer_at, o.delivered_at) end    as delivered_temp,
                -- delivered_temp gets renamed to delivered_at after the usage in by derived delivered, this so to prevent that deireved_deliverd depends on the delivered_at which exists in prep_supply_docouments.

                coalesce(case
                             when delivered_temp is not null then delivered_temp
                             when shipped_to_customer_at + interval '7 days' < current_date and
                                  delivered_temp is null then shipped_to_customer_at + interval '7 days' end,
                         o.completed_at)                                                           as derived_delivered_at,

                delivered_temp as delivered_at,

                case
                    when cdt.order_uuid is not null then cdt.cdtd_shipped_from_cross_dock_at
                    when is_cross_docking_ind and has_shipment_delivered_to_crossdock_date_consecutive
                        then ab.shipment_delivered_to_crossdock_at end                             as delivered_to_cross_dock_at


from {{ ref('prep_supply_documents') }} as soq
    left join {{ ref('stg_orders_geo') }} as sog on soq.order_uuid = sog.order_uuid
    left join agg_batches as ab on soq.order_uuid = ab.order_uuid
    left join supply_cdt as cdt on cdt.order_uuid = soq.order_uuid
    left join {{ ref('prep_supply_orders') }} as o on o.uuid = soq.order_uuid
    left join agg_shipment as agg_s on soq.order_uuid = agg_s.order_uuid
    left join {{ ref('prep_supply_documents') }} as oq on oq.uuid = o.quote_uuid
where soq.type = 'purchase_order'
  and soq.is_last_version
