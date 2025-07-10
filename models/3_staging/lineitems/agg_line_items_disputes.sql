with 

distinct_dispute_description as (
    select distinct 
        line_item_uuid, 
        created, 
        description
    from {{ ref('sources_network', 'gold_dispute_line_items_issues') }}
)
select gdlii.line_item_uuid,
       min(gdlii.created)                           as dispute_created_at,
       listagg('[' || date_trunc('day', ddd.created) || '] ' || trim(ddd.description),
           '\n\n')
           within group (order by ddd.created)    as dispute_description,
    --    listagg(distinct '[' || date_trunc('day', created) || '] ' || trim(description),
    --        '\n\n')
    --        within group (order by created)    as dispute_description, --- distinct inside listagg doesn't work in snowflake
       max(gdlii.affected_parts_quantity)           as dispute_affected_parts_quantity,
       max(case when gdlii.dispute_issue_id = 1 then 1 else 0 end) as has_tolerance_issue,
       max(case when gdlii.dispute_issue_id = 3 then 1 else 0 end) as has_incomplete_order_issue,
       max(case when gdlii.dispute_issue_id = 5 then 1 else 0 end) as has_missing_threads_issue,
       max(case when gdlii.dispute_issue_id = 7 then 1 else 0 end) as has_other_issue,
       max(case when gdlii.dispute_issue_id = 2 then 1 else 0 end) as has_surface_finish_issue,
       max(case when gdlii.dispute_issue_id = 4 then 1 else 0 end) as has_incorrect_material_issue,
       max(case when gdlii.dispute_issue_id = 6 then 1 else 0 end) as has_parts_damaged_issue
from {{ ref('sources_network', 'gold_dispute_line_items_issues') }}  gdlii
    left join distinct_dispute_description ddd on gdlii.line_item_uuid = ddd.line_item_uuid
group by 1