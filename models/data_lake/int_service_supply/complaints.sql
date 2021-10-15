select created created_at,
       updated,
       deleted,
       line_item_uuid,
       created_by_user_id,
       reviewed_by_user_id,
       {{ varchar_to_boolean('is_valid') }},
       {{ varchar_to_boolean('is_conformity_issue') }},
       comment,
       outcome_customer,
       outcome_supplier,
       resolution_datetime resolution_at,
       number_of_parts
from {{ source('int_service_supply', 'complaints') }}