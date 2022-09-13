{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}


select
    c.created_at, 
    c.line_item_uuid, 
    u.first_name + ' ' + u.last_name created_by,
    ur.first_name + ' ' + ur.last_name reviewed_by,
    c.is_valid, 
    c.is_conformity_issue, 
    c.comment,
    c.outcome_customer, 
    c.outcome_supplier, 
    c.resolution_at, 
    c.number_of_parts,
    ct.name as reason_type,
    cr.name as reason,
    c.claim_type,
    c.liability
from {{ ref('complaints') }} c
left join {{ source('int_service_supply', 'complaint_type_reasons') }} ctr on ctr.complaint_uuid = c.line_item_uuid
left join {{ source('int_service_supply', 'complaint_types') }} ct on ctr.complaint_type_id = ct.id
left join {{ source('int_service_supply', 'complaint_type_reasons_association') }} ctra on ctra.complaint_type_reasons_id = ctr.id
left join {{ source('int_service_supply', 'complaint_reasons') }}  cr on cr.id = ctra.complaint_reason_id
left join {{ ref('users') }} u on u.user_id = c.created_by_user_id
left join {{ ref('users') }} ur on ur.user_id = c.reviewed_by_user_id
inner join {{ ref('prep_line_items') }} pli on pli.uuid = c.line_item_uuid
