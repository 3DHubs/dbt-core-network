{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

select a1.id,
       a1.created  as                   created_at,
       a1.order_uuid,
       q1.user_type,
       q1.question                      question_1,
       a1.answer::int                   answer_1,
       q2.question as                   question_2,
       a2.answer   as                   answer_2,
       u.first_name + ' ' + u.last_name reviewed_by
from {{ ref('order_review_answers') }} a1
         inner join {{ source('int_service_supply', 'order_review_questions') }} q1 on q1.id = a1.question_id and a1.question_id in (1, 3)
         left join {{ ref('users') }}  u on u.user_id = a1.user_id
         left join {{ ref('order_review_answers') }}  a2
                   on a2.order_uuid = a1.order_uuid and a1.user_id = a2.user_id and a2.question_id in (2, 4)
         left join {{ source('int_service_supply', 'order_review_questions') }} q2 on q2.id = a2.question_id
where true
