select owner_id,
       email,
       first_name,
       last_name,
       user_id,
       trunc(created_at::timestamp) as created_at,
       trunc(updated_at::timestamp) as updated_at,
       archived,
       primary_team_name as hubspot_team_name -- primary (default) team name
from {{ source('data_lake', 'hubspot_owners') }}
where is_current
