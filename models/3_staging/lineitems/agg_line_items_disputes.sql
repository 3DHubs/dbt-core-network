select sqli.uuid                                   as line_item_uuid,
       min(sdli.created)                           as dispute_created_at,
       listagg(distinct '[' || trunc(sdli.created) || '] ' || trim(sdli.description),
           '\n\n')
           within group (order by sdli.created)    as dispute_description,
       max(sdli.affected_parts_quantity)           as dispute_affected_parts_quantity,
       max(case when sdi.id = 1 then 1 else 0 end) as has_tolerance_issue,
       max(case when sdi.id = 3 then 1 else 0 end) as has_incomplete_order_issue,
       max(case when sdi.id = 5 then 1 else 0 end) as has_missing_threads_issue,
       max(case when sdi.id = 7 then 1 else 0 end) as has_other_issue,
       max(case when sdi.id = 2 then 1 else 0 end) as has_surface_finish_issue,
       max(case when sdi.id = 4 then 1 else 0 end) as has_incorrect_material_issue,
       max(case when sdi.id = 6 then 1 else 0 end) as has_parts_damaged_issue
from {{ ref('prep_line_items') }} as sqli
            inner join {{ ref('prep_dispute_line_items') }} sdli on sqli.uuid = sdli.line_item_uuid
            left join {{ source('int_service_supply', 'disputes') }} sd on sd.uuid = sdli.dispute_uuid
            left join {{ source('int_service_supply', 'dispute_issues') }} sdi on sdli.dispute_issue_id = sdi.id
where sd.status = 'new'
group by 1