select id,
       first_name,
       last_name,
       email,
       envelope_id,
       quote_uuid,
       created,
       updated,
       completed_at,
       hubspot_warning_sent_at

from {{ source('int_service_supply', 'quote_docusign_requests') }}