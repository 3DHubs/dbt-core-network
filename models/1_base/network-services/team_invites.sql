 select invite_created_at,
        invite_updated_at,
        invite_id,
        email,
        invite_status,
        team_id,
        team_name,
        team_created_at
 from {{ ref('sources_network', 'gold_team_invites') }}
 where is_latest