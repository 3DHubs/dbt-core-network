{{ config(materialized="table") }}

select
    ow.id::bigint as owner_id,
    ow.email as email,
    initcap(ow.first_name) as "first_name",
    replace(initcap(ow.last_name), ' Pl', '')
    + case when t.name::varchar ilike '%protolabs%' then ' (PL)' else '' end as last_name, --todo-migration-test: replaced ~ for ilike
    initcap(first_name) + ' ' + replace(initcap(ow.last_name), ' Pl', '') + case when t.name::varchar ilike '%protolabs%' then ' (PL)' else '' end as name, --todo-migration-test: replaced ~ for ilike
    ow.user_id as user_id,
    ow.created_at as created_at,
    ow.updated_at as updated_at,
    ow.archived as archived,
    ow.load_timestamp as loaded_at,
    t.name::varchar as primary_team_name,
    cast(t.primary as boolean) as is_current
from {{ source("ext_hubspot", "hubspot_owners") }} ow, ow.teams t --todo-migration-research Join notation of tables; I'm assuming is json doesn't work in snowflake
where t.primary = true
