select dli.*,
       dlii.dispute_issue_id
from {{ source('int_service_supply', 'dispute_line_items') }} as dli
left join {{ source('int_service_supply', 'dispute_line_items_issues') }} as dlii
on dli.uuid = dlii.dispute_line_item_uuid