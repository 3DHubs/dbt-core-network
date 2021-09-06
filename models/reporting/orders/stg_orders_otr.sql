----------------------------------------------------------------
-- ON TIME RATE (OTR)
----------------------------------------------------------------

-- Sources:
-- 1. CNC Orders
-- 2. STG Documents
-- 3. STG Logistics
-- 4. Reporting Fact Delays


-- Suppliers Submit a form when an order is Delayed

with delays as (
    select order_uuid,
           min(submitted_at) as first_delay_submitted_at
    from {{ ref('fact_delays') }}
    group by 1
)

-- Main Query: It compares shipping dates with promised shipping date from order documents (PO & Quote)

select distinct orders.uuid       as order_uuid,

       case
           when docs.promised_shipping_at_by_supplier is null then null
           when orders.status in ('completed', 'delivered', 'disputed', 'shipped') and logistics.shipped_at is null
               then null
           when orders.status = 'canceled' then null
           when logistics.shipped_at > docs.promised_shipping_at_by_supplier then false
           when logistics.shipped_at <= docs.promised_shipping_at_by_supplier then true
           when logistics.shipped_at is null and dateadd(day, 1, docs.promised_shipping_at_by_supplier) < current_date then false
           else null end    is_shipped_on_time_by_supplier,

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
           else null end as is_shipped_on_time_to_customer,

       round(extract(minutes
                     from (logistics.shipped_to_customer_at - orders.promised_shipping_date)) / 1440,
             1)          as shipping_to_customer_delay_days,

       round(extract(minutes
                     from (logistics.shipped_at - docs.promised_shipping_at_by_supplier)) / 1440,
             1)          as shipping_by_supplier_delay_days,

       delays.first_delay_submitted_at


from {{ ref('cnc_orders') }} as orders
left join {{ ref ('stg_orders_documents')}} as docs
    on orders.uuid = docs.order_uuid
left join {{ ref ('stg_orders_logistics')}} as logistics on orders.uuid = logistics.order_uuid
left join delays on orders.uuid = delays.order_uuid
