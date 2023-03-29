----------------------------------------------------------------
-- ON TIME RATE (OTR)
----------------------------------------------------------------

-- Sources:
-- 1. CNC Orders
-- 2. STG Documents
-- 3. STG Logistics
-- 4. Reporting Fact Delays
-- 5. Prep Supply Buffers


-- Suppliers submit a form when an order is delayed

with delay_aggregates as (
    select order_uuid,
           count(*) as number_of_delays,
           true as has_delay_notifications,
           bool_or(delay_liability='supplier') as has_delay_liability_supplier,
           min(delay_created_at) as first_delay_created_at
    from {{ ref('fact_delays') }}
    group by 1
)

-- Main Query: It compares shipping dates with promised shipping date from order documents (PO & Quote)

select distinct orders.uuid                              as order_uuid,

                case
                    when docs.po_active_promised_shipping_at_by_supplier is null then null
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_at is null
                        then null
                    when orders.status = 'canceled' then null
                    when logistics.shipped_at > docs.po_active_promised_shipping_at_by_supplier then false
                    when logistics.shipped_at <= docs.po_active_promised_shipping_at_by_supplier then true
                    when logistics.shipped_at is null and
                         dateadd(day, 1, docs.po_active_promised_shipping_at_by_supplier) < current_date then false
                    else null
                    end                                  as is_shipped_on_time_by_supplier,

                -- This code is used to give suppliers a 12 hours window after the customer promised by date to
                -- Hand over the product to the carrier (this should be removed at a later stage).
                case 
                    when docs.po_active_promised_shipping_at_by_supplier is not null then
                        dateadd(hour, 12, shipment_received_by_carrier_at)    
                    else null
                end as promised_shipping_at_by_supplier_pick_up_adjusted,
                
                case
                    when promised_shipping_at_by_supplier_pick_up_adjusted is null then null
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_at is null
                        then null
                    when orders.status = 'canceled' then null
                    when logistics.shipment_received_by_carrier_at > promised_shipping_at_by_supplier_pick_up_adjusted then false
                    when logistics.shipment_received_by_carrier_at <= promised_shipping_at_by_supplier_pick_up_adjusted then true
                    when logistics.shipment_received_by_carrier_at is null and
                         dateadd(day, 1, promised_shipping_at_by_supplier_pick_up_adjusted) < current_date then false
                    else null
                    end                                  as is_picked_up_on_time_from_supplier,

                case
                    when orders.promised_shipping_date is null then null
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_to_customer_at is null then null
                    when orders.status = 'canceled' then null
                    when logistics.shipped_to_customer_at > orders.promised_shipping_date then false
                    when logistics.shipped_to_customer_at <= orders.promised_shipping_date then true
                    when logistics.shipped_to_customer_at is null and
                         dateadd(day, 1, orders.promised_shipping_date) < current_date
                        then false
                    else null
                    end                                  as is_shipped_on_time_to_customer,

                case
                    when orders.promised_shipping_date is null then null
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipment_to_customer_received_by_carrier_at is null then null
                    when orders.status = 'canceled' then null
                    when logistics.shipment_to_customer_received_by_carrier_at > orders.promised_shipping_date
                        then false
                    when logistics.shipment_to_customer_received_by_carrier_at <= orders.promised_shipping_date
                        then true
                    when logistics.shipment_to_customer_received_by_carrier_at is null and
                         dateadd(day, 1, orders.promised_shipping_date) < current_date
                        then false
                    else null
                    end                                  as is_pick_up_on_time_to_customer,

                round(extract(minutes from (logistics.shipped_to_customer_at - orders.promised_shipping_date)) / 1440,
                      1)                                 as shipping_to_customer_delay_days,

                round(extract(minutes from (logistics.shipped_at - docs.po_active_promised_shipping_at_by_supplier)) / 1440,
                      1)                                 as shipping_by_supplier_delay_days,

                -- Delay Notification Feature Aggregates
                dagg.has_delay_notifications,
                dagg.number_of_delays,
                dagg.has_delay_liability_supplier,
                dagg.first_delay_created_at,

                -- Buffer value
                buffers.first_leg_buffer_value

from {{ ref('prep_supply_orders') }} as orders
    left join {{ ref ('stg_orders_documents')}} as docs on orders.uuid = docs.order_uuid
    left join {{ ref ('stg_orders_logistics')}} as logistics on orders.uuid = logistics.order_uuid
    left join {{ ref ('prep_supply_buffers')}}  as buffers on docs.sourced_at::date = buffers.date and 
    case when logistics.origin_country not in ('United States', 'China', 'India', 'Mexico') then 'Row' else logistics.origin_country end = buffers.supplier_country and logistics.cross_dock_country = buffers.crossdock_country
    left join delay_aggregates as dagg on orders.uuid = dagg.order_uuid
