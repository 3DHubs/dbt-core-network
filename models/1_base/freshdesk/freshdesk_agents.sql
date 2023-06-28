with
    agents as ( select *, row_number() over (partition by id order by load_timestamp nulls last) as rn
                from {{ source('ext_freshdesk', 'freshdesk_agents') }} fal )
select agents.id,
       agents.ticket_scope,
       ftsm.description as ticket_scope_description,
       agents.created_at,
       agents.updated_at,
       agents.last_active_at,
       agents.available_since,
       agents.type,
       agents.agent_level_id,
       agents.available,
       agents.occasional,
       agents.contact_active,
       agents.contact_email,
       agents.contact_job_title,
       agents.contact_language,
       agents.contact_last_login_at,
       agents.contact_mobile,
       agents.contact_name,
       agents.contact_phone,
       agents.contact_time_zone,
       agents.contact_created_at,
       agents.contact_updated_at,
       agents.contact_avatar_id::bigint as contact_avatar_id,
       agents.contact_avatar_name,
       agents.contact_avatar_content_type,
       agents.contact_avatar_size::float::bigint as contact_avatar_size,
       agents.contact_avatar_created_at,
       agents.contact_avatar_updated_at,
       agents.contact_avatar_attachment_url,
       agents.contact_avatar_thumb_url,
       agents.signature,
       decode(agents.rn, 1, True) as _is_latest,
       agents.load_timestamp as _load_timestamp
from agents
left join {{ ref('seed_freshdesk_ticket_scope') }} as ftsm
       on agents.ticket_scope = ftsm.scope_id