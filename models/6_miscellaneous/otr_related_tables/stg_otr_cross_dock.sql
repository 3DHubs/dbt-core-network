-- ---------------------
-- Cross Dock component
-- ---------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022
-- Use case:
-- This table determines per order how long it spent in cross dock.
select
    fo.order_uuid,
    'Cross Docking' as otr_source,
    'Cross Docking Time' as otr_impact,
    'Transit time at cross dock' otr_process,
    'Tracking Number' as related_document_type,
    '' related_record,
    fo.delivered_to_cross_dock_at as start_date,
    fo.shipped_from_cross_dock_at as end_date,
    date_diff('minute', fo.delivered_to_cross_dock_at, fo.shipped_from_cross_dock_at)
    / 60.0 as hours_in_type,
    hours_in_type - 24 as hours_late,
    'Time allocated for cross dock to process the order' as notes
from {{ ref("fact_orders") }} as fo
