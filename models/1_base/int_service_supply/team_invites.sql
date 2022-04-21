 select created,
        updated,
        deleted,
        id,
        email,
        status,
        team_id,
        invited_by_id,
        rank() over (partition by email order by created desc) as rnk_desc_email -- used to determine last invite of email
 from {{ source('int_service_supply', 'team_invites') }}