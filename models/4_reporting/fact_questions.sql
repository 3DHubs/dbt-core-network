with
union_question_feature as (

    -- dimension questions
    select
        q.uuid               as question_uuid,
        q.order_uuid,
        q.line_item_uuid,
        q.status             as question_status,
        q.submitted_at,
        q.author_id,
        q.author_full_name as author_name,
        q.answered_by_full_name as answered_by_name,
        q.answered_by_internal as answered_by_hubs,
        q.answered_by_email as answered_email,
        q.answered_at,
        q.answered_by_id,
        'dimension_question' as question_type,
        null                 as question_description,
        q.reply_description || ' ' || q.reply_value || q.reply_unit as answer,
        false                as has_attachment,
        null::integer        as material_id,
        null::integer        as material_subset_id,
        null::integer        as material_color_id,
        null                 as finish_slug,
        null::integer        as tolerance_id,
        null::boolean        as has_threads,
        null::boolean        as has_part_marking,
        null::boolean        as has_internal_corners

    from {{ ref('network_services', 'gold_questions') }} as q
        inner join {{ ref('prep_supply_documents') }} as psd on q.purchase_order_uuid = psd.uuid and psd.is_active_po

    union all

    -- open questions
    select
        bq.uuid,
        bq.order_uuid,
        bq.line_item_uuid,
        bq.status,
        bq.submitted_at,
        bq.author_id,
        bq.author_full_name as author_name,
        bq.answered_by_full_name as answered_by_name,
        bq.answered_by_internal as answered_by_hubs,
        bq.answered_by_email as answered_email,        
        bq.answered_at,
        bq.answered_by_id,
        'open_question'                       as question_type,
        bq.question_text                      as question_description,
        bq.answer_text                        as answer,
        bq.answer_attachment_uuid is not null as has_attachment,
        null::integer                         as material_id,
        null::integer                         as material_subset_id,
        null::integer                         as material_color_id,
        null                                  as finish_slug,
        null::integer                         as tolerance_id,
        null::boolean                         as has_threads,
        null::boolean                         as has_part_marking,
        null::boolean                         as has_internal_corners

    from {{ ref('network_services', 'gold_open_questions') }} as bq
        inner join {{ ref('prep_supply_documents') }} as psd on bq.purchase_order_uuid = psd.uuid and psd.is_active_po

    union all

    -- part feature questions
    select
        pf.uuid                               as question_uuid,
        pf.order_uuid,
        pf.line_item_uuid,
        pf.status                             as question_status,
        pf.submitted_at,
        pf.author_id,
        pf.author_full_name as author_name,
        pf.answered_by_full_name as answered_by_name,
        pf.answered_by_internal as answered_by_hubs,
        pf.answered_by_email as answered_email,      
        pf.answered_at,
        pf.answered_by_id,
        case
            when pf.type = 'change_request' then 'part_feature_change_request'
            when pf.type = 'competing_specifications' then 'part_feature_competing_specifications'
        end                                   as question_type,
        pf.question_text                      as questions_description,
        pf.answer_text                        as answer,
        pf.answer_attachment_uuid is not null as has_attachment,
        pf.material_id,
        pf.material_subset_id,
        pf.material_color_id,
        pf.finish_slug,
        pf.tolerance_id,
        pf.has_threads,
        pf.has_part_marking,
        pf.has_internal_corners

    from {{ ref('network_services', 'gold_part_feature_questions') }} as pf
        inner join {{ ref('prep_supply_documents') }} as psd on pf.purchase_order_uuid = psd.uuid and psd.is_active_po
)

select
    uqf.*,
    round(datediff(minute, uqf.submitted_at::timestamp, uqf.answered_at::timestamp) * 1.0 / 1440, 1) as question_response_time
from union_question_feature as uqf
