select 
       id                                                     as inspection_id,
       caliper_id,
       comments                                               as inspection_comments,

       -- Timestamp Fields
       completed_at                                           as inspection_completed_at,
       created_at                                             as inspection_created_at,
       time_spent::decimal                                    as inspection_time_spent_seconds,
       case 
            when paused_time_total > 0 then TIMESTAMP 'epoch' + paused_at * INTERVAL '1 second'     
            else null end                                     as inspection_paused_at,
       paused_time_total::decimal                             as inspection_total_paused_time_seconds,
       
       doc_status                                             as document_status,
       document_number,
       inspection_result,
       inspection_type,
       inspector                                              as inspector_name,
       location                                               as inspection_location,
       packaging_issues,
       status                                                 as inspection_status,

       -- Boolean Fields
       {{ varchar_to_boolean('in_hubs_box')}}                 

from {{ source('int_retool', 'qc_inspections') }} as qc_inspections

