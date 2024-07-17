select 
    id                                                        as line_item_inspection_id,
    inspection_id,
    comments                                                  as line_item_inspection_comments,
    created_at                                                as line_item_inspection_created_at,
    document_number,
    line_item_number,
    line_item_title,
    line_item_uuid,
    number_inspected                                          as line_item_quantity_inspected,
    quantity                                                  as line_item_quantity,
    quantity_affected                                         as line_item_quantity_affected,

    -- Boolean Fields
    {{ varchar_to_boolean('dimensional_issues')}},             
    {{ varchar_to_boolean('fit_issues')}},                     
    {{ varchar_to_boolean('thread_issues')}},                  
    {{ varchar_to_boolean('visual_issues')}},                  
    {{ varchar_to_boolean('inspected')}},                      
    {{ varchar_to_boolean('passed')}},                         
    {{ varchar_to_boolean('smart_qc')}}                        

from {{ source('int_retool', 'inspection_line_items') }} as qc_inspections_line_items
