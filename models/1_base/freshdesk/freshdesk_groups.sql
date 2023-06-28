with groups as (
    select *,
           row_number() over (partition by id order by updated_at desc, load_timestamp desc) as rn
    from {{ source('ext_freshdesk', 'freshdesk_groups') }}
),
     fd_groups as (
         select groups.id,
                groups.name,
                groups.description,
                groups.escalate_to::bigint      as escalate_to,
                groups.unassigned_for,
                groups.business_hour_id::bigint as business_hour_id,
                groups.group_type,
                groups.created_at,
                groups.updated_at,
                groups.auto_ticket_assign,
                False                           as _is_legacy_group,
                decode(groups.rn, 1, True)      as _is_latest,
                groups.load_timestamp           as _load_timestamp
         from groups)
select *
from fd_groups
union all
select *
from {{ source('ext_freshdesk', 'freshdesk_groups_legacy_20200401') }}