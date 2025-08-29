{{ config(materialized="table") }}

--todo-migration-test: refactored this model because of semi-structured
--todo-migration-research: this might change when updating the ingestion method

with owners_flat as (
  select
      owners.id::bigint                  as owner_id,
      owners.email                       as email,
      initcap(owners.first_name)         as first_name,
      initcap(owners.last_name)          as last_name_raw,        -- keep raw
      team.value:name::string            as team_name,
      team.value:primary::boolean        as is_current,
      owners.user_id                     as user_id,
      owners.created_at                  as created_at,
      owners.updated_at                  as updated_at,
      owners.archived                    as archived,
      owners.load_timestamp              as loaded_at
  from {{ source("ext_hubspot", "hubspot_owners") }} owners,
       lateral flatten(input => parse_json(owners.teams)) team
), final as (

-- Simple: includes last_name_raw
select
    *,
  replace(coalesce(last_name_raw,''), ' Pl', '') ||
    case when team_name ilike '%protolabs%' then ' (PL)' else '' end as last_name,
  concat_ws(' ',
    first_name,
    replace(coalesce(last_name_raw,''), ' Pl', '') ||
      case when team_name ilike '%protolabs%' then ' (PL)' else '' end
  ) as owner_name
from owners_flat
where is_current
)
select *
from final
