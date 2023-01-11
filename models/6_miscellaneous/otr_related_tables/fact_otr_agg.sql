-----------------------------------
-- Fact Order Aggregation
-----------------------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022

-- Use case:
-- This table aggregates the data originating from the stg_otr tables.
-- The output should provides an overview of the delay cause.

select
    fo.order_uuid,
    fo.is_shipped_on_time_to_customer,
    date_diff('minute', fo.promised_shipping_at_to_customer,  fo.shipped_to_customer_at)/60.0 as actual_hours_late,
    date_diff('minute', fo.sourced_at,  fo.shipped_to_customer_at)/60.0 as actual_total_lead_time,
    sum(case when not fo.is_shipped_on_time_to_customer then foa.hours_late else 0 end) as total_calculated_hours_late,
    sum(case when not fo.is_shipped_on_time_to_customer and foa.otr_source = 'Sourcing' then foa.hours_late else 0 end) sourcing_late_impact,
    sum(case when not fo.is_shipped_on_time_to_customer and foa.otr_source = 'Production' then foa.hours_late else 0 end) production_late_impact,
    sum(case when not fo.is_shipped_on_time_to_customer and foa.otr_source = 'Logistics' then foa.hours_late else 0 end) logistics_late_impact,
    sum(case when not fo.is_shipped_on_time_to_customer and foa.otr_source = 'Cross Docking' then foa.hours_late else 0 end) cross_dock_late_impact,
    sum(foa.hours_in_type) as calculated_total_lead_time,
    sum(case when foa.otr_source = 'Sourcing' then foa.hours_in_type else 0 end) sourcing_hours,
    sum(case when foa.otr_source = 'Production' then foa.hours_in_type else 0 end) production_hours,
    sum(case when foa.otr_source = 'Logistics' then foa.hours_in_type else 0 end) logistics_hours,
    sum(case when foa.otr_source = 'Cross Docking' then foa.hours_in_type else 0 end) cross_dock_hours,
    actual_hours_late - total_calculated_hours_late as uncategorized_hours_late
from {{ ref ('fact_orders') }} as fo
left join {{ ref ('fact_otr_components') }} as foa on fo.order_uuid = foa.order_uuid
group by 1, 2, 3, 4