{{ config(materialized="table") }}

select
    ow.id::bigint as owner_id,
    ow.email as email,
    initcap(ow.first_name) as "first_name",
    replace(initcap(ow.last_name), ' Pl', '')
    + case when lower(t.name::varchar) ~ 'protolabs' then ' (PL)' else '' end as last_name,
    initcap(first_name)
    + ' '
    + replace(initcap(ow.last_name), ' Pl', '')
    + case when lower(t.name::varchar) ~ 'protolabs' then ' (PL)' else '' end as name,
    ow.user_id as user_id,
    ow.created_at as created_at,
    ow.updated_at as updated_at,
    ow.archived as archived,
    ow.load_timestamp as loaded_at,
    t.name::varchar as primary_team_name,
    t.primary::bool as is_current
from {{ source("ext_hubspot", "hubspot_owners") }} ow, ow.teams t
where t.primary = true
