select interaction_id,
       ticket_id,
       created_date as created_at,
       interaction_type,
       agent_interaction_count,
       internal_interaction_count,
       customer_interaction_count,
       is_first_interaction
from {{ ref('fact_freshdesk_interactions') }}
where created_date >= dateadd('year', -2, date_trunc('year', current_date)) --todo-migration-test dateadd current_date
