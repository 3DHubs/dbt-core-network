select created created_at,
       updated,
       line_item_uuid,
       created_by_user_id,
       reviewed_by_user_id,
       {{ varchar_to_boolean('is_valid') }},
       {{ varchar_to_boolean('is_conformity_issue') }},
       comment,
       outcome_customer,
       outcome_supplier,
       resolution_datetime resolution_at,
       number_of_parts,
       claim_type,
       liability,
       {{ varchar_to_boolean('corrective_action_plan_needed') }},
       qc_comment


from {{ source('int_service_supply', 'complaints') }}