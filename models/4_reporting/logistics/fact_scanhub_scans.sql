Select 

    uuid,
    location,
    scanner_name,
    created_at,
    updated_at,
    archived_at,
    created_at_ams,
    updated_at_ams,
    archived_at_ams,
    created_at_localized,
    updated_at_localized,
    archived_at_localized,
    document_number,
    tracking_number,
    is_manual,
    message_text,
    ship_urgency

   

from {{ ref('scanhub_scans') }} 