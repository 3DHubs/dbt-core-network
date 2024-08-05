{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

select
    created_at, 
    line_item_uuid, 
    created_by,
    reviewed_by,
    is_valid, 
    is_conformity_issue, 
    comment,
    outcome_customer, 
    outcome_supplier, 
    resolution_at, 
    number_of_parts,
    reason_type,
    reason,
    claim_type,
    liability,
    corrective_action_plan_needed,
    qc_comment
from {{ ref('complaints_reasons') }}
