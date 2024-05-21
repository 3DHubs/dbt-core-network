Select

    inspection_id,
    caliper_id,
    document_number,
    document_status,
    in_hubs_box                                                        as is_in_pl_box,
    inspection_comments,
    inspection_completed_at,
    inspection_created_at,
    inspection_location,
    inspection_paused_at,
    inspection_result,
    inspection_status,
    inspection_type,
    inspection_total_paused_time_seconds,
    inspection_time_spent_seconds,
    inspector_name,
    packaging_issues
    
from {{ ref('qc_inspections') }} as qc_inspections