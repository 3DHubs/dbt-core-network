select line_item_uuid,
       min(created)                           as dispute_created_at,
       listagg(distinct '[' || trunc(created) || '] ' || trim(description),
           '\n\n')
           within group (order by created)    as dispute_description,
       max(affected_parts_quantity)           as dispute_affected_parts_quantity,
       max(case when dispute_issue_id = 1 then 1 else 0 end) as has_tolerance_issue,
       max(case when dispute_issue_id = 3 then 1 else 0 end) as has_incomplete_order_issue,
       max(case when dispute_issue_id = 5 then 1 else 0 end) as has_missing_threads_issue,
       max(case when dispute_issue_id = 7 then 1 else 0 end) as has_other_issue,
       max(case when dispute_issue_id = 2 then 1 else 0 end) as has_surface_finish_issue,
       max(case when dispute_issue_id = 4 then 1 else 0 end) as has_incorrect_material_issue,
       max(case when dispute_issue_id = 6 then 1 else 0 end) as has_parts_damaged_issue
from {{ ref('network_services', 'gold_dispute_line_items_issues') }}
group by 1