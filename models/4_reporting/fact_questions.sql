-- Maintained by Daniel Salazar
-- Last edit on 2022-03-06

-- The question feature handles 2 different questions curated questions and open questions
with
    union_question_feature as (
        -- curated questions
        select
            q.uuid as question_uuid,
            q.order_uuid,
            q.line_item_uuid,
            q.status as question_status,
            q.submitted_at,
            q.author_id,
            q.answered_at,
            q.answered_by_id,
            q.title  as question_type,
            NULL as question_description,
            qro.description
            || ' '
            || qr.value
            || qr.unit
            || ' '
            || qr.is_correct as answer,
            false as has_attachment

        from {{ source('int_service_supply', 'questions') }} as q
        left join {{ source('int_service_supply', 'replies') }} as qr on q.uuid = qr.question_uuid
        left join {{ source('int_service_supply', 'reply_options') }} as qro on qr.reply_option_id = qro.id
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
            bq.type as question_type,
            bq.question_text as question_description,
            bq.answer_text as answer,
            bq.answer_attachment_uuid is not null as has_attachment
        from {{ source('int_service_supply', 'base_question') }} as bq
        group by 2,3,4,5,6,7,8,9,10,11,12
    )

select
    uqf.*,
    round(
        extract(minutes from (uqf.answered_at - uqf.submitted_at)) / 1440, 1
    ) as question_response_time,
    u_auth.first_name + ' ' + u_auth.last_name as author_name,
    u_answ.first_name + ' ' + u_answ.last_name as answered_name,
    u_answ.is_internal                         as answered_by_hubs,
    u_answ.email                                as answered_email
from union_question_feature as uqf
left join {{ ref('prep_users') }} as u_auth on uqf.author_id = u_auth.user_id
left join {{ ref('prep_users') }} as u_answ on uqf.answered_by_id = u_answ.user_id
