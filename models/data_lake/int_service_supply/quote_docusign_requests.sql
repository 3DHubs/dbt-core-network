select id,
       md5(first_name) as first_name_hashed,
       md5(last_name)  as last_name_hashed,
       md5(email)      as email_hashed,
       envelope_id,
       quote_uuid,
       created,
       updated,
       completed_at,
       hubspot_warning_sent_at
from int_service_supply.quote_docusign_requests