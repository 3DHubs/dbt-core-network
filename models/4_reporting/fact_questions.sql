with
    union_question_feature as (

        -- dimension questions
        select
            q.uuid as question_uuid,
            q.order_uuid,
            q.line_item_uuid,
            q.status as question_status,
            q.submitted_at,
            q.author_id,
            q.answered_at,
            q.answered_by_id,
            'dimension_question'  as question_type,
            null as question_description,
            qro.description
            || ' '
            || qr.value
            || qr.unit as answer,
            false as has_attachment,
            null::integer as material_id,
            null::integer as material_subset_id,
            null::integer as material_color_id,
            null as finish_slug,
            null::integer as tolerance_id,
            null::boolean as has_threads,
            null::boolean as has_part_marking,
            null::boolean as has_internal_corners

        from {{ ref('questions') }} as q
        inner join {{ ref('prep_supply_documents') }} psd on q.purchase_order_uuid = psd.uuid and is_active_po
        left join {{ ref('replies') }} as qr on q.uuid = qr.question_uuid
        left join {{ ref('reply_options') }} as qro on qr.reply_option_id = qro.id
        where decode(qr.is_correct, 'true', true, 'false', false)

        union all

        -- open questions
        select
            max(bq.uuid) uuid,
            bq.order_uuid,
            bq.line_item_uuid,
            bq.status,
            bq.submitted_at,
            bq.author_id,
            bq.answered_at,
            bq.answered_by_id,
            'open_question' as question_type,
            bq.question_text as question_description,
            bq.answer_text as answer,
            bq.answer_attachment_uuid is not null as has_attachment,
            null::integer as material_id,
            null::integer as material_subset_id,
            null::integer as material_color_id,
            null as finish_slug,
            null::integer as tolerance_id,
            null::boolean as has_threads,
            null::boolean as has_part_marking,
            null::boolean as has_internal_corners

        from {{ ref('open_questions') }} as bq
        inner join {{ ref('prep_supply_documents') }} psd on bq.purchase_order_uuid = psd.uuid and is_active_po
        group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20


        union all

        -- part feature questions
        select
            pf.uuid as question_uuid,
            pf.order_uuid,
            pf.line_item_uuid,
            pf.status as question_status,
            pf.submitted_at,
            pf.author_id,
            pf.answered_at,
            pf.answered_by_id,
            case when pf.type = 'change_request' then 'part_feature_change_request'
                when pf.type = 'competing_specifications' then 'part_feature_competing_specifications' end as question_type,
            pf.question_text as questions_description,
            pf.answer_text as answer,
            pf.answer_attachment_uuid is not null as has_attachment,
            pf.material_id,
            pf.material_subset_id,
            pf.material_color_id,
            pf.finish_slug,
            pf.tolerance_id,
            pf.has_threads,
            pf.has_part_marking,
            pf.has_internal_corners

        from {{ ref('part_feature_questions') }} as pf
        inner join {{ ref('prep_supply_documents') }} psd on pf.purchase_order_uuid = psd.uuid and is_active_po
    )

select
    uqf.*,round(date_diff('minutes',uqf.submitted_at,uqf.answered_at )*1.0/1440,1) as question_response_time,
    u_auth.first_name + ' ' + u_auth.last_name as author_name,
    u_answ.first_name + ' ' + u_answ.last_name as answered_name,
    u_answ.is_internal                         as answered_by_hubs,
    u_answ.email                                as answered_email
from union_question_feature as uqf
left join {{ ref('prep_users') }} as u_auth on uqf.author_id = u_auth.user_id
left join {{ ref('prep_users') }} as u_answ on uqf.answered_by_id = u_answ.user_id
