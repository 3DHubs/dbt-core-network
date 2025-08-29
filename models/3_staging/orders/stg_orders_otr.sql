----------------------------------------------------------------
-- ON TIME RATE (OTR)
----------------------------------------------------------------

-- Sources:
-- 1. CNC Orders
-- 2. STG Documents
-- 3. STG Logistics
-- 4. Reporting Fact Delays
-- 5. Prep Supply Buffers
-- 6. Delay probability


-- Suppliers submit a form when an order is delayed

with delay_aggregates as (
    select order_uuid,
           count(*) as number_of_delays,
           true as has_delay_notifications,
           --todo-migration-test boolor_agg
           boolor_agg(delay_liability='supplier') as has_delay_liability_supplier,
           min(delay_created_at) as first_delay_created_at,
           max(new_shipping_at) as latest_new_shipping_at
    from {{ ref('fact_delays') }}
    group by 1
),
    delay_pred as (
            select 
                order_uuid,
                round(predicted_proba,4) as delay_probability,
                predicted_class as delay_days_predicted,
                row_number() over (partition by order_uuid order by model_executed_at asc) as rn
            from {{ source('int_analytics', 'delay_probability_v2') }}  pred
        )

-- Main Query: It compares shipping dates with promised shipping date from order documents (PO & Quote)

select distinct orders.uuid                              as order_uuid,

                case
                    when docs.po_active_promised_shipping_at_by_supplier > getdate() then null
                    when docs.po_active_promised_shipping_at_by_supplier = null then null --todo-migration-test
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipment_label_created_at = null --todo-migration-test
                        then null
                    when orders.status = 'canceled' then null
                    when logistics.shipment_label_created_at > docs.po_active_promised_shipping_at_by_supplier then false
                    when logistics.shipment_label_created_at <= docs.po_active_promised_shipping_at_by_supplier then true
                    when logistics.shipment_label_created_at = null and --todo-migration-test
                         dateadd(day, 1, docs.po_active_promised_shipping_at_by_supplier) < current_date then false
                    else null
                    end                                  as is_shipped_on_time_by_supplier_label_created,

                -- This code is used to give suppliers a 12 hours window after the customer promised by date to
                -- Hand over the product to the carrier (this should be removed at a later stage).
                case 
                    when docs.po_active_promised_shipping_at_by_supplier <> null then --todo-migration-test
                        dateadd(hour, 12, docs.po_active_promised_shipping_at_by_supplier)    
                    else null
                end as promised_shipping_at_by_supplier_pick_up_adjusted,
                
                case
                    -- IM S-OTR Logic Part
                    when coalesce(psd.technology_id,hs.hubspot_technology_id) = '3' and orders.created < '2024-10-01' then null
                    when coalesce(psd.technology_id,hs.hubspot_technology_id) = '3' and lower(hs.im_deal_type) = 'tooling & samples' and orders.status != 'canceled' then
                    case
                        when logistics.shipped_at <= hs.im_hs_promised_shipping_at_by_supplier then true
                        when logistics.shipped_at > hs.im_hs_promised_shipping_at_by_supplier then false
                        when logistics.shipped_at = null and dateadd(day, 1, hs.im_hs_promised_shipping_at_by_supplier) < getdate() then false --todo-migration-test
                        end -- End of IM S-OTR Logic Part
                    when dateadd(day, 1, po_active_promised_shipping_at_by_supplier) > getdate() then null
                    when promised_shipping_at_by_supplier_pick_up_adjusted = null then null --todo-migration-test
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_at = null --todo-migration-test
                        then null
                    when orders.status = 'canceled' then null
                    when logistics.shipped_at > promised_shipping_at_by_supplier_pick_up_adjusted then false
                    when logistics.shipped_at <= promised_shipping_at_by_supplier_pick_up_adjusted then true
                    when logistics.shipped_at = null and --todo-migration-test
                         dateadd(day, 1, promised_shipping_at_by_supplier_pick_up_adjusted) < getdate() then false
                    else null
                    end                                  as is_shipped_on_time_by_supplier, -- Switched to as main calculation in July 2023
                case
                    when dateadd(day, 1, orders.promised_shipping_date) > getdate() then null
                    when orders.promised_shipping_date = null then null --todo-migration-test
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_to_customer_at = null then null --todo-migration-test
                    when orders.status = 'canceled' then null
                    when hs.delay_reason = 'customer_requested_hold' and logistics.shipped_to_customer_at > orders.promised_shipping_date  then null
                    when logistics.shipped_to_customer_at > orders.promised_shipping_date then false
                    when logistics.shipped_to_customer_at <= orders.promised_shipping_date then true
                    when logistics.shipped_to_customer_at = null and dateadd(day, 1, orders.promised_shipping_date) < getdate() --todo-migration-test
                        then false
                    else null
                    end                                  as is_shipped_on_time_to_customer,

                           case
                    when dateadd(day, 1, orders.expected_shipping_date) > getdate() then null
                    when orders.expected_shipping_date = null then null --todo-migration-test
                    when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and
                         logistics.shipped_to_customer_at = null then null --todo-migration-test
                    when orders.status = 'canceled' then null
                    when hs.delay_reason = 'customer_requested_hold' and logistics.shipped_to_customer_at > orders.expected_shipping_date  then null
                    when logistics.shipped_to_customer_at > orders.expected_shipping_date then false
                    when logistics.shipped_to_customer_at <= orders.expected_shipping_date then true
                    when logistics.shipped_to_customer_at = null and dateadd(day, 1, orders.expected_shipping_date) < getdate() --todo-migration-test
                        then false
                    else null
                    end                                  as is_shipped_on_time_expected_by_customer,

                    --todo-migration-test datediff
                    round(datediff('minutes',orders.promised_shipping_date,logistics.shipped_to_customer_at )*1.0/1440,1) as shipping_to_customer_delay_days,
                    round(datediff('minutes',promised_shipping_at_by_supplier_pick_up_adjusted,logistics.shipped_at )*1.0/1440,1) as shipping_by_supplier_delay_days,



                -- Delay Notification Feature Aggregates
                dagg.has_delay_notifications,
                dagg.number_of_delays,
                dagg.has_delay_liability_supplier,
                dagg.first_delay_created_at,
                dagg.latest_new_shipping_at,

                -- Delay probability prediction
                dp.delay_probability,
                dp.delay_days_predicted,

                -- Buffer value
                buffers.first_leg_buffer_value

from {{ ref('prep_supply_orders') }} as orders
    left join {{ ref ('stg_orders_documents')}} as docs on orders.uuid = docs.order_uuid
    left join {{ ref('prep_supply_documents') }} as psd on orders.quote_uuid = psd.uuid
    left join {{ ref ('stg_orders_logistics')}} as logistics on orders.uuid = logistics.order_uuid
    left join {{ ref ('stg_orders_hubspot')}} as hs on orders.hubspot_deal_id = hs.hubspot_deal_id
    left join {{ ref ('prep_supply_buffers')}}  as buffers on docs.sourced_at::date = buffers.date and 
    case when logistics.origin_country not in ('United States', 'China', 'India', 'Mexico') then 'Row' else logistics.origin_country end = buffers.supplier_country and logistics.cross_dock_country = buffers.crossdock_country
    left join delay_aggregates as dagg on orders.uuid = dagg.order_uuid
    left join delay_pred dp on dp.order_uuid = orders.uuid and dp.rn=1
