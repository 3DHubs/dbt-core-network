select
created_at,
line_item_uuid,
is_valid,
is_conformity_issue,
outcome_customer,
outcome_supplier,
resolution_at,
created_by,
reviewed_by,
comment,
claim_type,
liability,
corrective_action_plan_needed,
qc_comment
from {{ ref('sources_network', 'gold_complaints') }}