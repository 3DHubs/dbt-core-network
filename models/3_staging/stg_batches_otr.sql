----------------------------------------------------------------
-- ON TIME RATE for batch shipments, new logic introduce in February 2024
----------------------------------------------------------------

-- Sources:
-- 1. STG Order Documents
-- More details of the batches can be found in fact_batches


with batch_pk_line_items as (
    select
        package_line_item_id,
        rank() over (partition by batch_shipment_line_item_id order by id asc) as batch_package_rank,
        batch_shipment_line_item_id
    from {{ ref ('batch_package_line_items') }}
),

package_lines as (
    select
        package_line_item_id,
        batch_shipment_line_item_id
    from batch_pk_line_items
    where batch_package_rank = 1
),

prep_batch_otr as (
    select
        docs.order_uuid,
        docs.order_quote_uuid,
        docs.po_active_uuid,
        fb.batch_uuid,
        bs.batch_number,
        po_bs.ship_by_date                                                                                              as promised_shipping_at_by_supplier,
        convert_timezone(
            geo.origin_timezone, promised_shipping_at_by_supplier
        )                                                                                                               as localized_promised_shipping_at_by_supplier,
        coalesce(
            fb.carrier_received_shipment_to_crossdock_at, fb.carrier_received_shipment_to_customer_at
        )                                                                                                               as shipped_by_supplier_at,
        convert_timezone(
            geo.origin_timezone, shipped_by_supplier_at
        )                                                                                                               as localized_shipped_by_supplier_at,
        bs.ship_by_date                                                                                                 as promised_shipping_at_to_customer,
        convert_timezone(
            geo.destination_timezone, promised_shipping_at_to_customer
        )                                                                                                               as localized_promised_shipping_at_to_customer,
        fb.carrier_received_shipment_to_customer_at                                                                     as shipped_to_customer_at,
        fb.delivered_to_crossdock_at,
        fb.provided_estimate_delivery_to_crossdock_at                                                                   as estimated_delivery_to_cross_dock_at,
        fb.shipped_from_cross_dock_at,
        fb.delivered_to_customer_at                                                                                     as delivered_at,
        coalesce(case
            when fb.delivered_to_customer_at <> null then fb.delivered_to_customer_at
            when
                shipped_to_customer_at + interval '7 days' < current_date
                and fb.delivered_to_customer_at = null then shipped_to_customer_at + interval '7 days'
        end)                                                                                                            as derived_delivered_at, --todo-migration-test = from is
        -- This code is used to give suppliers a 12 hours window after the customer promised by date to
        -- Hand over the product to the carrier (this should be removed at a later stage).
        case
            when promised_shipping_at_by_supplier <> null
                then
                    dateadd(hour, 12, promised_shipping_at_by_supplier)
        end                                                                                                             as promised_shipping_at_by_supplier_adjusted, --todo-migration-test = from is

        case
            when dateadd(day, 1, promised_shipping_at_by_supplier) > getdate() then null
            when promised_shipping_at_by_supplier_adjusted = null then null
            when
                orders.status in ('completed', 'delivered', 'disputed', 'shipped')
                and shipped_by_supplier_at = null then null
            when orders.status = 'canceled' then null
            when shipped_by_supplier_at > promised_shipping_at_by_supplier_adjusted then false
            when shipped_by_supplier_at <= promised_shipping_at_by_supplier_adjusted then true
            when
                shipped_by_supplier_at = null
                and dateadd(day, 1, promised_shipping_at_by_supplier_adjusted) < getdate() then false
        end                                                                                                             as is_shipped_on_time_by_supplier, -- Switched to as main calculation in July 2023 --todo-migration-test = from is

        case
            when dateadd(day, 1, promised_shipping_at_to_customer) > getdate() then null
            when promised_shipping_at_to_customer = null then null
            when
                orders.status in ('completed', 'delivered', 'disputed', 'shipped')
                and shipped_to_customer_at = null then null
            when orders.status = 'canceled' then null
            when hs.delay_reason = 'customer_requested_hold' and shipped_to_customer_at > promised_shipping_at_to_customer then null
            when shipped_to_customer_at > promised_shipping_at_to_customer then false
            when shipped_to_customer_at <= promised_shipping_at_to_customer then true
            when shipped_to_customer_at = null and dateadd(day, 1, promised_shipping_at_to_customer) < getdate()
                then false
        end                                                                                                             as is_shipped_on_time_to_customer, --todo-migration-test = from is

        round(
            date_diff('minutes', promised_shipping_at_to_customer, shipped_to_customer_at) * 1.0 / 1440, 1
        )                                                                                                               as shipping_to_customer_delay_days,
        round(
            date_diff('minutes', promised_shipping_at_by_supplier_adjusted, shipped_by_supplier_at) * 1.0 / 1440, 1
        )                                                                                                               as shipping_by_supplier_delay_days,
        first_value(bs.batch_number) over (
            partition by docs.order_uuid order by bs.batch_number desc rows between unbounded preceding and unbounded following
        )                                                                                                               as last_batch_number,
        coalesce(bs.batch_number = last_batch_number, false)                                                            as is_last_batch,
        sum(po_bsli.quantity)                                                                                           as quantity_target,
        sum(po_pli.quantity)                                                                                            as quantity_package,
        sum(po_bsli.fulfilled_quantity)                                                                                 as quantity_fulfilled,
        rank() over (partition by docs.order_uuid, bs.batch_number order by shipped_by_supplier_at asc, ppo.status asc) as multi_po_rank --Sometimes a package line item is associate to an older active PO. This will select the batch that has associate batch / package uuid shipping info.

    from {{ ref('prep_supply_orders') }} as orders
        left join {{ ref ('stg_orders_documents') }} as docs on orders.uuid = docs.order_uuid
        left join {{ ref ('prep_purchase_orders') }} as ppo on orders.uuid = ppo.order_uuid
        inner join {{ ref ('batch_shipments') }} as bs on docs.order_quote_uuid = bs.quote_uuid
        inner join {{ ref ('batch_shipments') }} as po_bs
            on ppo.uuid = po_bs.quote_uuid and bs.batch_number = po_bs.batch_number
        left join {{ ref ('batch_shipment_line_items') }} as po_bsli
            on po_bs.id = po_bsli.batch_shipment_id
        left join package_lines as po_bpli on po_bsli.id = po_bpli.batch_shipment_line_item_id
        left join {{ ref ('package_line_items') }} as po_pli
            on po_bpli.package_line_item_id = po_pli.id
        left join {{ ref('fact_batches') }} as fb on po_pli.package_uuid = fb.batch_uuid
        left join {{ ref ('stg_orders_hubspot') }} as hs on orders.hubspot_deal_id = hs.hubspot_deal_id
        left join {{ ref ('stg_orders_geo') }} as geo on orders.uuid = geo.order_uuid
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, ppo.status
)

select * from prep_batch_otr where multi_po_rank = 1
