-- ______ ___  _____ _____   _____ ___________ 
-- |  ___/ _ \/  __ \_   _| |  _  |_   _| ___ \
-- | |_ / /_\ \ /  \/ | |   | | | | | | | |_/ /
-- |  _||  _  | |     | |   | | | | | | |    / 
-- | |  | | | | \__/\ | |   \ \_/ / | | | |\ \ 
-- \_|  \_| |_/\____/ \_/    \___/  \_/ \_| \_|


-- Doom font asci                                                         
-- Model from March 2024, where the new batches (multiple batch per order, otr calculation comes together with the legacy otr calculation)

select
    'batch'                          as source,
    true                             as is_batch_shipment,
    order_uuid,
    order_quote_uuid,
    po_active_uuid,
    batch_uuid,
    batch_number,
    promised_shipping_at_by_supplier,
    localized_promised_shipping_at_by_supplier,
    shipped_by_supplier_at,
    localized_shipped_by_supplier_at,
    promised_shipping_at_to_customer,
    localized_promised_shipping_at_to_customer,
    promised_shipping_at_to_customer as expected_shipping_at_to_customer, -- to be developed still.
    shipped_to_customer_at,
    delivered_to_crossdock_at,
    estimated_delivery_to_cross_dock_at,
    shipped_from_cross_dock_at,
    quantity_target,
    quantity_package,
    quantity_fulfilled,
    is_shipped_on_time_by_supplier,
    is_shipped_on_time_to_customer,
    is_shipped_on_time_to_customer   as is_shipped_on_time_expected_by_customer, -- to be developed still.
    shipping_to_customer_delay_days,
    shipping_by_supplier_delay_days,
    is_last_batch
from {{ ref('stg_batches_otr') }}
union all
select
    'orders'                   as source,
    false                      as is_batch_shipment,
    order_uuid,
    order_quote_uuid,
    po_active_uuid,
    order_uuid + 1::varchar    as batch_uuid,
    1                          as batch_number,
    promised_shipping_at_by_supplier,
    localized_promised_shipping_at_by_supplier,
    order_shipped_at           as shipped_by_supplier_at,
    localized_order_shipped_at as localized_shipped_by_supplier_at,
    promised_shipping_at_to_customer,
    localized_promised_shipping_at_to_customer,
    pso.expected_shipping_date as expected_shipping_at_to_customer,
    shipped_to_customer_at,
    delivered_to_cross_dock_at,
    estimated_delivery_to_cross_dock_at,
    shipped_from_cross_dock_at,
    total_quantity             as quantity_target,
    total_quantity             as quantity_package,
    total_quantity             as quantity_fulfilled,
    is_shipped_on_time_by_supplier,
    is_shipped_on_time_to_customer,
    is_shipped_on_time_expected_by_customer,
    shipping_to_customer_delay_days,
    shipping_by_supplier_delay_days,
    true as is_last_batch
from {{ ref('stg_fact_orders') }} as sfo
    left join {{ ref('prep_supply_orders') }} as pso on sfo.order_uuid = pso.uuid
where
    order_uuid not in (select order_uuid from {{ ref('stg_batches_otr') }})
    and is_sourced
