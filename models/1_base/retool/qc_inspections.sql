select 
       id,                                                     
       caliper_id,
       comments,

       -- Timestamp Fields
       completed_at,
       created_at,
       time_spent::decimal                                    as inspection_time_spent_seconds,
       case 
            when paused_time_total > 0 then TIMESTAMP 'epoch' + paused_at * INTERVAL '1 second'     
            else null end                                     as paused_at,
       paused_time_total::decimal                             as total_inspection_paused_time_seconds,
       
       doc_status                                             as document_status,
       document_number,
       inspection_result,
       inspection_type,
       inspector                                              as inspector_name,
       location,                                              
       packaging_issues,
       status,

       -- Boolean Fields
       {{ varchar_to_boolean('in_hubs_box')}}                 

from {{ ref('dbt_src_external', 'gold_int_airbyte_retool_qc_inspections') }} as qc_inspections

