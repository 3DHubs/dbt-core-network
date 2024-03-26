
--    __           _     _           _       _               
--   / _|         | |   | |         | |     | |              
--  | |_ __ _  ___| |_  | |__   __ _| |_ ___| |__   ___  ___ 
--  |  _/ _` |/ __| __| | '_ \ / _` | __/ __| '_ \ / _ \/ __|
--  | || (_| | (__| |_  | |_) | (_| | || (__| | | |  __/\__ \
--  |_| \__,_|\___|\__| |_.__/ \__,_|\__\___|_| |_|\___||___/

-- Doom font asci                                                         
-- Model from March 2024, where the new batches (multiple batch per order, otr calculation comes together with the legacy otr calculation)

select 
       'batch' as source,
       true as is_batch_shipment,
       order_uuid,
       order_quote_uuid,
       po_active_uuid,
       batch_uuid,
       batch_number,
       promised_shipping_at_by_supplier,
       shipped_by_supplier_at,
       promised_shipping_at_to_customer,
       promised_shipping_at_to_customer as expected_shipping_at_to_customer, -- to be developed still.
       shipped_to_customer_at,
       quantity_target,
       quantity_package,
       quantity_fulfilled,
       is_shipped_on_time_by_supplier,
       is_shipped_on_time_to_customer,
       is_shipped_on_time_to_customer as is_shipped_on_time_expected_by_customer, -- to be developed still.
       shipping_to_customer_delay_days,
       shipping_by_supplier_delay_days
from {{ ref('stg_batches_otr') }} 
union all
select
       'orders' as source,
       false as is_batch_shipment,
       order_uuid,
       order_quote_uuid,
       po_active_uuid,
       null as batch_uuid,
       1 as batch_number,
       promised_shipping_at_by_supplier,
       order_shipped_at as shipped_by_supplier_at,
       promised_shipping_at_to_customer,
       pso.expected_shipping_date as expected_shipping_at_to_customer,
       shipped_to_customer_at,
       total_quantity as quantity_target,
       total_quantity as quantity_package,
       total_quantity as quantity_fulfilled,
       is_shipped_on_time_by_supplier,
       is_shipped_on_time_to_customer,
       is_shipped_on_time_expected_by_customer,
       shipping_to_customer_delay_days,
       shipping_by_supplier_delay_days
from {{ ref('stg_fact_orders') }}  sfo  
left join {{ ref('prep_supply_orders') }}  pso on pso.uuid = sfo.order_uuid
where order_uuid not in (select order_uuid from {{ ref('stg_batches_otr') }} )
and is_sourced
