select        
       id,
       contact_email,
       contact_name,
       ticket_scope,
       _load_timestamp
from {{ ref( 'dbt_src_external', 'gold_airbyte_freshdesk_agents') }} agents
left join {{ ref('seed_freshdesk_ticket_scope') }} as ftsm
       on agents.ticket_scope = ftsm.scope_id