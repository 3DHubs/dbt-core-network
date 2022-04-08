{{ config(materialized='table') }}

select owner_id::bigint as owner_id,
       email,
       initcap(first_name) as "first_name",
       initcap(last_name) + case when primary_team_name ~ 'Protolabs' then ' (PL)' else '' end as last_name,
       initcap(first_name) + ' ' +
       initcap(last_name) + case when primary_team_name ~ 'Protolabs' then ' (PL)'else '' end as name,
       user_id,
       created_at,
       updated_at,
       archived,
       primary_team_name,
       load_timestamp,
       start_date,
       end_date,
       is_current
from  {{ source('data_lake', 'hubspot_owners') }}
where is_current