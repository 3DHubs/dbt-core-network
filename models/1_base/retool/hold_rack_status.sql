select 
    id,
    document_number, 
    xhr_status                                                                          as hold_rack_status, 
    usr_comment                                                                         as comment, 
    updated_on                                                                          as updated_at, 
    cast(status_date as timestamp)                                                      as status_at, 
    inspection_id, 
    inspector, 
    archived_by
               

from {{ source('int_stitch_retool', 'xdock_holdrack_status') }} as hold_rack_status

