{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

--fetching courier data
with shipment_carrier_supplier as (
    select package_uuid,
           car.name as carrier_from_supplier
    from  {{ source('int_service_supply', 'shipments') }} shp
             left join  {{ source('int_service_supply', 'shipping_carriers') }} car on car.id = shp.tracking_carrier_id
    where shipping_leg in ('drop_shipping:customer', 'cross_docking:warehouse')
    order by package_uuid
),
     shipment_carrier_customer_first_value as (
         --some shipments have multiple packages created for a single leg of shipping
         select package_uuid,
                (first_value(car.name) over (partition by package_uuid
                    order by created asc
                    rows between unbounded preceding and unbounded following)) as carrier_from_cross_dock
         from {{ source('int_service_supply', 'shipments') }} shp
                  left join {{ source('int_service_supply', 'shipping_carriers') }} car on car.id = shp.tracking_carrier_id
         where shipping_leg in ('cross_docking:customer')
         order by package_uuid
     ),
     shipment_carrier_customer as (
         select package_uuid,
                carrier_from_cross_dock
         from shipment_carrier_customer_first_value
         group by 1, 2),

     shipment_carrier as (
         select p.uuid as package_uuid,
                carrier_from_supplier,
                carrier_from_cross_dock
         from {{ ref('packages') }} p
                  left join shipment_carrier_supplier scs on p.uuid = scs.package_uuid
                  left join shipment_carrier_customer scc on p.uuid = scc.package_uuid
     ),
--fetching packages data
     supply_packages as (
         select p.uuid                                         as package_uuid,
                p.order_uuid,
                p.created                                      as package_create_date,
                ready_for_pickup_at                            as package_ready_for_pickup_date,
                transit_to_warehouse_at                        as package_shipped_to_crossdock_date,
                at_warehouse_at                                as package_delivered_to_crossdock_date,
                transit_to_customer_at                         as package_shipped_to_customer_date,
                p.delivered_at                                 as package_delivered_to_customer_date,
                case when not is_partial then delivered_at end as full_delivered_date,
                case when is_partial then 1 else 0 end         as is_partial
         from  {{ ref('packages') }} p
         where p.status <> 'draft'
     ),
     supply_package_inconsistencies as (
         -- check packages for inconsistentcies: shipping date before package create date or shipping dates not consecutive
         select p.order_uuid,
                package_uuid,
                case when is_partial = 1 then true else false end                         as is_partial_shipment,
                package_create_date,
                package_ready_for_pickup_date,
                package_shipped_to_crossdock_date,
                package_delivered_to_crossdock_date,
                package_shipped_to_customer_date,
                package_delivered_to_customer_date,
                full_delivered_date,
                case when oq.is_cross_docking then true else false end                    as is_cross_docking_ind,
                case
                    when is_cross_docking_ind and package_shipped_to_crossdock_date is not null and
                         package_delivered_to_crossdock_date is not null
                        and package_shipped_to_customer_date is not null and
                         package_delivered_to_customer_date is not null then true
                    when (is_cross_docking_ind = false or is_cross_docking is null) and
                         package_shipped_to_customer_date is not null and package_delivered_to_customer_date is not null
                        then true else false end                                                     as package_shipping_dates_complete,
                row_number() over (partition by o.uuid order by package_create_date asc ) as package_number,
                case
                    when is_cross_docking_ind is false then null
                    when date_trunc('day', package_create_date) >
                         date_trunc('day', package_shipped_to_crossdock_date) then false
                    else true end                                                         as package_shipped_to_crossdock_after_package_created,
                case
                    when is_cross_docking_ind is false then null
                    when date_trunc('day', package_create_date) >
                         date_trunc('day', package_delivered_to_crossdock_date) then false
                    else true end                                                         as package_delivered_to_crossdock_after_package_created,
                case
                    when date_trunc('day', package_create_date) >
                         date_trunc('day', package_shipped_to_customer_date) then false
                    else true end                                                         as package_shipped_to_customer_after_package_created,
                case
                    when date_trunc('day', package_create_date) >
                         date_trunc('day', package_delivered_to_customer_date) then false
                    else true end                                                         as package_delivered_to_customer_after_package_created,

                case
                    when is_cross_docking_ind is false then null
                    when is_cross_docking_ind and
                         package_shipped_to_crossdock_date > package_delivered_to_crossdock_date then false
                    else true end                                                         as package_delivered_to_crossdock_date_consecutive,
                case
                    when is_cross_docking_ind is false then true
                    when is_cross_docking_ind and
                         package_delivered_to_crossdock_date > package_shipped_to_customer_date then false
                    when is_cross_docking_ind and
                         package_shipped_to_crossdock_date > package_shipped_to_customer_date then false
                    else true end                                                         as package_shipped_to_customer_date_consecutive,
                case
                    when is_cross_docking_ind is false and
                         package_shipped_to_customer_date > package_delivered_to_customer_date then false
                    when is_cross_docking_ind and
                         package_shipped_to_customer_date > package_delivered_to_customer_date then false
                    when package_delivered_to_crossdock_date > package_delivered_to_customer_date then false
                    when package_shipped_to_crossdock_date > package_delivered_to_customer_date then false
                    else true end                                                         as package_delivered_to_customer_date_consecutive,

                --consistent shipping dates
                case
                    when package_shipped_to_crossdock_date is null then false
                    when is_cross_docking_ind and package_shipped_to_crossdock_after_package_created is true
                        then true
                    else false end                                                        as has_consistent_shipped_to_crossdock_dates,
                case
                    when package_shipped_to_customer_date is null then false
                    when package_shipped_to_customer_after_package_created is true
                        and package_shipped_to_customer_date_consecutive then true
                    else false end                                                        as has_consistent_shipped_to_customer_dates,
                case
                    when package_delivered_to_crossdock_date is null then false
                    when is_cross_docking_ind and package_delivered_to_crossdock_after_package_created is true
                        and package_delivered_to_crossdock_date_consecutive is true then true
                    else false end                                                        as has_consistent_delivered_to_crossdock_dates,
                case
                    when package_delivered_to_customer_date is null then false
                    when package_delivered_to_customer_after_package_created is true
                        and package_delivered_to_customer_date_consecutive is true then true
                    else false end                                                        as has_consistent_delivered_to_customer_dates

         from supply_packages p
                  left join  {{ ref('prep_supply_orders') }} o on o.uuid = p.order_uuid
                  left join  {{ ref('prep_supply_documents') }} as oq on oq.uuid = o.quote_uuid
     )

select soq.order_uuid,
       sp.package_uuid,
       case when package_number = 1 then true else false end as is_first_package,
       is_cross_docking_ind as is_cross_docking,
       carrier_from_supplier,
       carrier_from_cross_dock,
       is_partial_shipment,
       package_ready_for_pickup_date,
       full_delivered_date,
       package_create_date,
       package_shipped_to_crossdock_date,
       package_delivered_to_crossdock_date,
       package_shipped_to_customer_date,
       package_delivered_to_customer_date,
       package_shipping_dates_complete,
       case
           when package_create_date is null then null
           when is_cross_docking_ind and has_consistent_shipped_to_crossdock_dates and
                has_consistent_delivered_to_crossdock_dates then true
           when is_cross_docking_ind = false and has_consistent_shipped_to_customer_dates and
                has_consistent_delivered_to_customer_dates then true
           else false end                                    as has_consistent_shipping_dates_from_supplier,

       case
           when package_create_date is null then null
           when is_cross_docking_ind is false then null
           when is_cross_docking_ind and has_consistent_shipped_to_customer_dates and
                has_consistent_delivered_to_customer_dates then true
           else false end                                    as has_consistent_shipping_dates_from_cross_dock,

       case
           when package_create_date is null then null
           when is_cross_docking_ind and has_consistent_shipped_to_crossdock_dates and
                has_consistent_delivered_to_crossdock_dates then true
           when is_cross_docking_ind and has_consistent_shipped_to_customer_dates and
                has_consistent_delivered_to_customer_dates then true
           when is_cross_docking_ind = false and has_consistent_shipped_to_customer_dates and
                has_consistent_delivered_to_customer_dates then true
           else false end                                    as has_consistent_shipping_dates


from supply_package_inconsistencies sp
         left join shipment_carrier sc on sc.package_uuid = sp.package_uuid
         left join {{ ref('prep_supply_documents') }} soq on soq.order_uuid = sp.order_uuid
         inner join {{ ref('prep_purchase_orders') }}pos on soq.uuid = pos.uuid

     --check if we still need the PO filter
where soq.type = 'purchase_order'
  and pos.status = 'active'
  --if we fail to create a shipment we also delete the package associated to the shipment.
  --that doesn't always seem to happen so this is a filter to make sure we only include packages that also have a shipment
  and sp.package_uuid in (
    select package_uuid
    from {{ source('int_service_supply', 'shipments') }})
