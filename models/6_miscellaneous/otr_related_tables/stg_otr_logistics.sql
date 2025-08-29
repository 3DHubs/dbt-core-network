-- --------------------
-- Logistics component
-- --------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022
-- Use case:
-- This table determines per order how long it spent in logistics.
select
    sfo.order_uuid,
    'Logistics' as otr_source,
    'Logistics Time' as otr_impact,
    'Transit time to cross dock' otr_process,
    'Tracking Number' as related_document_type,
    '' as related_record,
    sfo.order_shipped_at as start_date,
    sfo.delivered_to_cross_dock_at as end_date,
    --todo-migration-test datediff
    datediff(
        'minute', sfo.order_shipped_at, sfo.delivered_to_cross_dock_at
    )
    / 60.0 as hours_in_type,
    hours_in_type - sfo.first_leg_buffer_value * 24.0 as hours_late,
    'Time allocated for logistics to transit to cross dock' as notes
from {{ ref("stg_fact_orders") }} as sfo
