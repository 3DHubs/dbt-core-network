-- Check first users that are in accepted status (a user can be invited more often, we give prio to the accepted state)
with stg_invite_accepted as (
    select email,
           ti.status,
           min(created) created,
           min(updated) updated
    from {{ ref('team_invites') }} ti
    where status = 'Accepted' and ti.rnk_desc_email = 1
    group by 1, 2),
-- Get other invited users with different status
     stg_other_invites as (select hubspot_contact_id,
                                  ti.team_id,
                                  t.name          as team_name,
                                  t.created       as team_created_at,
                                  min(ti.created) as created,
                                  max(ti.status)  as status
                           from {{ ref('team_invites') }}  ti
                                    inner join {{ ref('prep_users') }} u on u.email = ti.email
                                    left join {{ source('int_service_supply', 'teams') }}  t on t.id = ti.team_id
                           where status in ('Pending', 'Revoked')
                             and ti.email not in (select email from stg_invite_accepted)  and ti.rnk_desc_email = 1
                           group by 1, 2, 3, 4)
-- Aggregate all users that are added to a team / invited to a team
select u.hubspot_contact_id,
       coalesce(t.name, stg_os.team_name)           as team_name,
       coalesce(t.id, stg_os.team_id)                    as team_id,
       coalesce(t.created, stg_os.team_created_at)     team_created_at,
       coalesce(min(stg_a.created), min(stg_os.created))   as invited_at, -- added min to avoid users with same hubspot contact id that were both invited as user, example 370382101
       coalesce(min(stg_a.updated))                      as invite_accepted_at,
       lower(coalesce(min(stg_a.status), min(stg_os.status))) as invite_status
from {{ ref('prep_users') }} u
         left join {{ source('int_service_supply', 'team_users') }} tu on tu.user_id = u.user_id
         left join {{ source('int_service_supply', 'teams') }} t on t.id = tu.team_id
         left join stg_invite_accepted stg_a on stg_a.email = u.email
         left join stg_other_invites stg_os on stg_os.hubspot_contact_id = u.hubspot_contact_id
where u.hubspot_contact_id is not null
  and u.hubspot_contact_id <> 0
  and (t.created is not null or stg_os.created is not null)
  and coalesce(t.id, stg_os.team_id) not in (4, 10, 75, 77, 124)
  and u.is_internal = false
  and u.rnk_desc_hubspot_contact_id = 1
  group by 1,2,3,4
