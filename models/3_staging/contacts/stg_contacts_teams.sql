-- Check first users that are in accepted status (a user can be invited more often, we give prio to the accepted state)
with stg_invite_accepted as (
    select
        email,
        ti.invite_status,
        min(ti.invite_created_at) as invite_created_at,
        min(ti.invite_updated_at) as invite_updated_at
    from {{ ref('team_invites') }} as ti
    where invite_status = 'Accepted'
    group by 1, 2
),

-- Get other invited users with different status
stg_other_invites as (
    select
        ti.email,
        ti.team_id,
        ti.team_name,
        ti.team_created_at,
        min(ti.invite_created_at) as invite_created_at,
        max(ti.invite_status)     as invite_status
    from {{ ref('team_invites') }} as ti
    where
        ti.invite_status in ('Pending', 'Revoked')
        and ti.email not in (select email from stg_invite_accepted)
    group by 1, 2, 3, 4
)

-- Aggregate all users that are added to a team / invited to a team
select
    u.hubspot_contact_id,
    coalesce(u.team_name, stg_os.team_name)                               as team_name,
    coalesce(u.team_id, stg_os.team_id)                                   as team_id,
    coalesce(u.team_created_at, stg_os.team_created_at)                   as team_created_at,
    coalesce(min(stg_a.invite_created_at), min(stg_os.invite_created_at)) as invited_at, -- added min to avoid users with same hubspot contact id that were both invited as user, example 370382101
    min(stg_a.invite_updated_at)                                          as invite_accepted_at,
    lower(coalesce(min(stg_a.invite_status), min(stg_os.invite_status)))  as invite_status
from {{ ref('prep_users') }} as u
    left join stg_invite_accepted as stg_a on u.email = stg_a.email
    left join stg_other_invites as stg_os on u.email = stg_os.email
where
    u.hubspot_contact_id is not null
    and u.hubspot_contact_id <> 0
    and (u.team_created_at is not null or stg_os.team_created_at is not null)
    and coalesce(u.team_id, stg_os.team_id) not in (4, 10, 75, 77, 124)
    and u.is_internal = false
    and u.rnk_desc_hubspot_contact_id = 1
group by 1, 2, 3, 4
