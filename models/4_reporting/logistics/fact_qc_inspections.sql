Select

    id,
    caliper_id,
    document_number,
    document_status,
    in_hubs_box                                                        as is_in_pl_box,
    comments,
    completed_at,
    created_at,
    location,
    paused_at,
    inspection_result,
    status,
    inspection_type,
    total_inspection_paused_time_seconds,
    inspection_time_spent_seconds,
    inspector_name,
    packaging_issues
    
from {{ ref('qc_inspections') }} as qc_inspections