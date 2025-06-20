select
    id,
    name,
    description,
    escalate_to,
    unassigned_for,
    business_hour_id,
    group_type,
    created_at,
    updated_at,
    auto_ticket_assign,
    _is_legacy_group,
    load_timestamp
from {{ ref('dbt_src_external', "gold_ext_airbyte_freshdesk_groups") }}
