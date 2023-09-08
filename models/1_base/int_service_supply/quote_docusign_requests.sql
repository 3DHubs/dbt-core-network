select max(id) id,  -- solution to avoid duplicate quote_uuids in the future
       first_name,
       max(last_name) last_name,
       email,
       max(envelope_id) envelope_id,
       quote_uuid,
       max(created) created,
       hubspot_warning_sent_at

from {{ source('int_service_supply', 'quote_docusign_requests') }}
group by 2,4,6,8