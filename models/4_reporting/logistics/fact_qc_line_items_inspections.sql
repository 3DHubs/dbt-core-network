Select

    line_item_inspection_id,
    inspection_id,
    line_item_inspection_comments,
    line_item_inspection_created_at,
    document_number,
    line_item_number,
    line_item_title,
    line_item_uuid,
    line_item_quantity_inspected,                                       
    line_item_quantity,
    line_item_quantity_affected,

    -- Boolean Fields
    dimensional_issues                                        as has_dimensional_issues,
    fit_issues                                                as has_fit_issues,
    thread_issues                                             as has_thread_issues,
    visual_issues                                             as has_visual_issues, 
    inspected                                                 as is_inspected,
    passed                                                    as is_passed,
    smart_qc                                                  as is_smart_qc   
    
from {{ ref('qc_inspections_line_items') }} as qc_inspections_line_items