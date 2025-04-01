select 
    id,
    orders.uuid                                                                     as order_uuid,
    document_number,  
    hold_rack_status, 
    comment, 
    updated_at, 
    status_at, 
    inspection_id, 
    inspector, 
    archived_by,
    min(status_at) over (partition by document_number) as date_entered,
    max(status_at) over (partition by document_number) as latest_contact

from {{ ref('hold_rack_status') }} rack
    left join {{ ref('prep_supply_orders') }} orders on rack.document_number = orders.number
