select 
       id,                                                     
       caliper_id,
       comments,

       -- Timestamp Fields
       completed_at,
       created_at,
       time_spent::decimal                                    as inspection_time_spent_seconds,
       case
        when paused_time_total > 0 then dateadd(second, paused_time_total, to_timestamp_ntz('1970-01-01 00:00:00'))
        else null 
        end                                                   as paused_at, --todo-migration-test: replaced multiplication of number by interval, to be checked; changed epoch -alec
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

from {{ ref('dbt_src_external', 'gold_retool_qc_inspections') }} as qc_inspections

